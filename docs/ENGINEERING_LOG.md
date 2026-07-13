# Engineering Log — FX Data Pipeline

Toto je záznam rozhodnutí, dôvodov a problémov, na ktoré som narazil pri budovaní tohto projektu. Cieľ: vedieť si to pred pohovorom prejsť a vysvetliť *prečo*, nielen *čo*.

---

## Fáza 0 — Repo, Docker, Postgres

**Rozhodnutia a prečo:**
- **`postgres:16` pinnutá verzia, nie `latest`** — reprodukovateľnosť buildov. O rok nechcem, aby mi CI bežalo proti inej major verzii s breaking changes, o ktorých neviem.
- **`.env` + `.env.example` pattern** — `.env` obsahuje skutočné hodnoty a nikdy nejde do gitu (`.gitignore`). `.env.example` je "kontrakt" — dokumentuje, aké premenné projekt potrebuje, bez skutočných hodnôt.
- **Named Docker volume (`postgres_data`) namiesto bind mountu** — dáta prežijú `docker compose down`, ale nie sú rozhajdákané ako súbory v repo priečinku, kde by som ich mohol omylom commitnúť.
- **Healthcheck v `docker-compose.yml` od začiatku** — vtedy sa zdal zbytočný, ale vo Fáze 3 naň Airflow čaká (`depends_on: condition: service_healthy`), takže infraštruktúra bola pripravená vopred.

**Problémy, ktoré som riešil:**
- SSH kľúč neexistoval (`ssh-add -l` → "no identities"), musel som ho vygenerovať (`ssh-keygen`) a nahrať na GitHub.
- Zlý formát git remote URL (`git@github.com/...` namiesto `git@github.com:...`) — chýbajúca dvojbodka spôsobovala "does not appear to be a git repository".
- Docker Desktop WSL integrácia nebola zapnutá — `docker` príkaz nebol nájdený vo WSL2, riešenie cez Docker Desktop Settings → Resources → WSL Integration.
- Používateľ nebol v `docker` skupine → `permission denied` na socket. Fix: `sudo usermod -aG docker $USER` + nová session.
- Git identity (`user.name`/`user.email`) nebola nastavená lokálne — commit zlyhal, kým som ju nenastavil.

**Pohovorové talking points:**
- Prečo `.env`/`.env.example` pattern a nie hardcoded credentials.
- Prečo healthcheck v compose súbore, keď naň nič spočiatku nečakalo.
- Ako debugovať SSH auth a Docker permission problémy na novom stroji (bežná onboarding zručnosť).

---

## Fáza 1 — Python ingestion (bronze layer)

**Zdroj dát:** Frankfurter API — zvolený pre: žiadny API key, žiadny praktický rate limit, ECB referenčné kurzy publikované raz denne (odôvodňuje `@daily` batch namiesto zbytočného real-time streamovania).

**Architektonické rozhodnutia:**
- **`config.py` používa `os.environ["X"]` (hranatá zátvorka), nie `.get("X")`, pre povinné premenné** — "fail fast": ak premenná chýba, program spadne hneď s jasnou chybou (`KeyError`), nie s `None`, ktorý by zlyhal nezrozumiteľne neskôr.
- **`logger.py` s `if not logger.handlers` guard** — zabraňuje duplicitným log riadkom pri opakovanom importe modulu.
- **Bronze tabuľka je append-only, BEZ unique constraintu** — zámerné. Bronze v medailónovej architektúre ukladá surové dáta presne tak, ako prišli, vrátane duplicít pri opakovanom behu. Deduplikácia je zodpovednosť transformačnej (dbt staging) vrstvy, nie ingestion vrstvy. Toto rozdelenie umožňuje kedykoľvek reprocessovať históriu bez rizika straty dát.
- **`sys.exit(1)` pri zlyhaní ingestion behu** — dôležité pre Airflow (Fáza 3): nenulový exit kód = zlyhaný task = spustí retry policy. Ak by som chybu len logoval a pokračoval, Airflow by si myslel že job prešiel, hoci dáta sa neuložili (tichý data loss).

**Overenie na živých dátach (nie len teoreticky):**
- Spustil som ingestion 2× → bronze malo 8 riadkov namiesto 4 → potvrdil som, že je to *správne* správanie (append-only), nie bug.

**Pohovorové talking points:**
- "Čo sa stane keď tvoj DAG beží dvakrát alebo sa reštartuje po zlyhaní?" → bronze append-only, duplicity sa riešia downstream, nič sa neprepíše/nestratí.
- "Prečo fail-fast config namiesto default hodnôt?" → chyby v produkcii sa majú prejaviť okamžite a zrozumiteľne, nie ticho pokračovať s nesprávnymi hodnotami.

---

## Fáza 2 — dbt transformácie (staging → intermediate → marts)

**Problém #1 — Python 3.14 vs dbt-core:** `dbt-core`/`mashumaro` knižnica ešte nepodporovala Python 3.14 (`UnserializableField` chyba). Riešenie: nainštalovaný Python 3.12 cez deadsnakes PPA popri systémovom 3.14, nové venv nad ním. **Talking point:** nie každý nástroj v stacku podporuje najnovší jazyk/runtime hneď po vydaní — dôvod, prečo produkčné tímy pinnujú Python verziu explicitne.

**Vrstvy a materializácie:**
- **staging/intermediate = `view`** (lacné na build, medzikroky, nikto ich priamo needotazuje)
- **marts = `table`** (finálny produkt, rýchle dopyty, netreba prepočítavať celý reťazec pri každom SELECTe)

**`stg_fx_rates` — deduplikácia cez `DISTINCT ON`:**
```sql
SELECT DISTINCT ON (base_currency, target_currency, rate_date)
    ...
ORDER BY base_currency, target_currency, rate_date, ingested_at DESC
```
Kľúčové pravidlo: `ORDER BY` musí začínať rovnakými stĺpcami ako `DISTINCT ON`, v rovnakom poradí, potom môže pridať stĺpec určujúci "kto vyhráva" v rámci skupiny.

**Overenie na živých dátach:** ingestoval som 2× (rovnaký deň) → bronze malo 12 riadkov → staging malo 8 (4 meny × 2 unikátne dni) → potvrdené, že deduplikácia funguje presne na úrovni `(mena, mena, dátum)`, nie na celom riadku.

**`int_fx_rate_changes` — `LAG()` window function:**
```sql
LAG(rate) OVER (PARTITION BY base_currency, target_currency ORDER BY rate_date) AS previous_rate
```
`PARTITION BY` drží históriu každého menového páru oddelene (žiadne krížové znečistenie medzi CHF a USD históriou). Prvý deň v každej partícii má `previous_rate = NULL` — to je correct, nie chyba (aritmetika s NULL vracia NULL, netreba to explicitne ošetrovať cez `CASE WHEN`).

**`fct_latest_fx_rates` — `ROW_NUMBER()` namiesto `DISTINCT ON`:**
Zámerne iný nástroj na rovnaký typ problému ("nájdi najnovší riadok v skupine") — `ROW_NUMBER()` je prenositeľnejší naprieč databázami (Redshift/BigQuery/Snowflake nemajú `DISTINCT ON`), zatiaľ čo `DISTINCT ON` je Postgres-špecifický. Dobré vedieť oba prístupy.

**Testovacia filozofia:**
- `not_null` testy len tam, kde dáva zmysel (nie na `previous_rate`/`rate_change` v `int_fx_rate_changes`, keďže NULL na prvý deň je legitímne, nie chyba — test by tam bol false positive).
- Singular testy (vlastné `.sql` súbory v `tests/`) na business-critical invarianty, ktoré vstavané generic testy nepokryjú (napr. "unikátnosť kombinácie 3 stĺpcov").
- `dbt build` namiesto `dbt run` + `dbt test` zvlášť — spustí modely aj testy v správnom poradí podľa DAG závislostí; ak test zlyhá, downstream modely sa nespustia.

**Výsledok:** 19 dbt testov, všetky PASS, `source()` vs `ref()` rozlíšenie zdokumentované cez `_sources.yml`.

**Pohovorové talking points:**
- Prečo `view` vs `table` materializácia podľa vrstvy.
- Prečo netestovať `not_null` mechanicky na každý stĺpec — testy majú odrážať skutočné biznis pravidlá.
- `DISTINCT ON` vs `ROW_NUMBER()` — kedy použiť ktorý, prenositeľnosť naprieč platformami.

---

## Fáza 3 — Airflow orchestrácia

**Verzia:** Airflow 3.1.2 — výrazne iná architektúra než staršie 2.x tutoriály (samostatný `api-server` namiesto `webserver`, nový `dag-processor` service).

**Kľúčové architektonické rozhodnutia:**
- **`LocalExecutor`, nie `CeleryExecutor`** — jeden pipeline na jednom stroji nepotrebuje distribuovaný task queue (Redis, workers). Executor sa vyberá podľa reálnej potreby škálovania, nie podľa "čo znie fancy".
- **Oddelená `airflow-postgres` metadata DB od `fx_pipeline` dátového warehousu** — orchestrátor a dátový store sú oddelené zodpovednosti, aj keď bežia na tom istom type databázy.
- **Vlastný `Dockerfile.airflow` s izolovaným dbt venv** (`/home/airflow/dbt_venv`) — `dbt-core` a `apache-airflow` majú konfliktné pinnuté Python závislosti (`click`, `jinja2`, `pydantic`). Riešenie: samostatné virtuálne prostredie vnútri Airflow image, DAG volá `dbt` binárku z tohto venv cez `BashOperator`, nie systémový `dbt`.
- **`BashOperator` namiesto natívneho dbt-Airflow balíčka (napr. Cosmos)** — jednoduchšie, transparentnejšie pre rozsah tohto projektu. Vedieť vysvetliť *prečo* je jednoduchšie riešenie namiesto automaticky siahnuť po najkomplexnejšom nástroji.

**Problémy, ktoré som riešil (v poradí, ako sa objavili):**
1. **`ModuleNotFoundError: No module named 'airflow'`** pri `airflow-init` — obišiel som vstavaný `/entrypoint` skript volaním `airflow ...` priamo namiesto `/entrypoint airflow ...`. Fix: vždy volať cez `/entrypoint`.
2. **`airflow users create` neexistuje v Airflow 3** — starý FAB-based user management bol nahradený "Simple Auth Manager" (default v Airflow 3). Users sa definujú cez `AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS: "airflow:admin"`, heslo sa generuje automaticky a vypíše v logoch `apiserver` kontajnera (`docker compose logs airflow-apiserver | grep -i password`).
3. **Zabudnuté uloženie `.env` úpravy vo VS Code** pred spustením terminálového príkazu — `.env` súbor na disku bol starý, kým editor "vyzeral" upravený. Poučenie: vždy over `cat` súboru priamo v termináli pred spoliehaním sa na editor.
4. **`psycopg2.OperationalError: connection to server at "localhost" ... Connection refused`** vnútri `ingest_fx_rates` tasku — `ingestion/config.py` čítal `POSTGRES_HOST=localhost` z `.env`, ale vnútri Airflow kontajnera `localhost` znamená samotný Airflow kontajner, nie `fx_postgres`. Fix: explicitný override `POSTGRES_HOST: postgres` v `environment:` bloku `docker-compose.yml` — premenné uvedené priamo v `environment:` majú prednosť pred tými z `env_file`.
5. **Rovnaký networking princíp riešený dvakrát** — raz pre dbt (`profiles.yml` cez `env_var('DBT_POSTGRES_HOST', 'localhost')`), raz pre Python ingestion (`POSTGRES_HOST` override) — jeden koncept: **Docker service name namiesto `localhost` vnútri kontajnerovej siete.**

**Overenie:** manuálny trigger DAG-u cez UI, všetky 3 tasky (`ingest_fx_rates → dbt_run → dbt_test`) prešli, 16 riadkov v bronze potvrdených priamo cez `psql`.

**Pohovorové talking points:**
- Prečo LocalExecutor vs Celery/Kubernetes — rozhodovanie podľa skutočnej potreby škálovania.
- Prečo oddelená metadata DB od dátového warehousu.
- Ako riešiť dependency konflikty medzi orchestrátorom a execution engine (izolované venv).
- Docker networking: `localhost` vs service name — kde presne "beží" `localhost` v kontexte kontajnera, a prečo to isté riešenie platí naprieč rôznymi nástrojmi (dbt, Python) v tom istom stacku.
- Ako diagnostikovať Airflow task zlyhanie: rozlíšiť Airflow-wrapper exception (generic "Bash command failed") od skutočného Python tracebacku vyššie v logu.

---

## Fáza 4 — CI/CD (GitHub Actions)

**Rozhodnutie:** GitHub Actions `services: postgres` namiesto vlastného `docker-compose` v CI — service containers bežia na `localhost` runnera priamo, netreba riešiť Docker network naming ako v Airflow (GitHub Actions to zjednodušuje presne pre tento účel).

**Workflow kroky:** checkout → setup Python 3.12 → nainštalovať ingestion závislosti → `pytest` → nainštalovať dbt → napísať `profiles.yml` na runneri → **seed bronze cez vlastný ingestion skript** (namiesto vymýšľania testovacích dát) → `dbt build`.

**Prečo seedovať cez ingestion skript, nie cez umelé test dáta:** dokazuje, že celý pipeline (ingestion + transformácia) funguje end-to-end pri každom push, nie len dbt izolovane.

**Pohovorové talking points:**
- Rozdiel medzi CI service containers a orchestračným Docker networkingom (Airflow).
- Prečo CI má testovať end-to-end reťazec, nie len jednotlivé komponenty izolovane.

---

*(Log sa priebežne dopĺňa s ďalšími fázami — Fáza 4 dokončenie, Fáza 5 Databricks/PySpark.)*
