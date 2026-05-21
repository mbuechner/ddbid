[![Java CI with Maven](https://github.com/mbuechner/ddbid/actions/workflows/maven.yml/badge.svg)](https://github.com/mbuechner/ddbid/actions/workflows/maven.yml) [![Docker](https://github.com/mbuechner/ddbid/actions/workflows/docker-publish.yml/badge.svg)](https://github.com/mbuechner/ddbid/actions/workflows/docker-publish.yml)

# DDBid

DDBid ist ein kleines Spring-Boot-Tool fuer DDB-IDs: Dumps ziehen, alte und neue ID-Welten vergleichen, fehlende und neue Entitaeten sichtbar machen. Kurz: ein freundlicher Differenzmotor fuer grosse CSV-Dateien, H2 und ein bisschen Chart.js.

## Was passiert hier?

- `ITEM`, `PERSON` und `ORGANIZATION` werden als komprimierte Dumps verarbeitet.
- Die Daten landen in einer lokalen H2-Datei.
- Die UI zeigt Tabellen, Downloads und Statistiken.
- Die Statistikseite cached aggregierte Werte; Cache-Refresh alle 4 Stunden.
- Bestehende Daten werden beim normalen Start nicht geloescht. Achtung: Der Import/Cron baut Tabellen neu auf; fuer reine Bestandsdaten also `SCHEDULER_ENABLED=false` setzen.

## Lokal starten

Entwicklungsmodus mit Spring Boot Maven Plugin:

```bash
./mvnw spring-boot:run
```

Unter Windows:

```powershell
.\mvnw.cmd spring-boot:run
```

Mit deaktiviertem Scheduler, wenn vorhandene Daten nur gelesen werden sollen:

```bash
./mvnw spring-boot:run -Dspring-boot.run.arguments="--scheduler.enabled=false"
```

Oder ueber `.env`:

```properties
SCHEDULER_ENABLED=false
```

Als gebautes Boot-Jar:

```bash
mvn -DskipTests package
java -jar target/ddbid.jar
```

Konfiguration kommt aus `application.properties`, Umgebungsvariablen oder einer `.env`-Datei via `springboot4-dotenv`.

Minimal nuetzliche `.env`:

```properties
DDBID_DATABASE_TYPE=h2
DDBID_DATABASE_FILE=data/ddbid_h2_DO_NOT_DELETE_ITS_IMPORTANT.db
DDBID_SECURITY_USER=user
DDBID_SECURITY_PASSWORD=password
SCHEDULER_ENABLED=true
```

`SCHEDULER_ENABLED` steuert die automatischen `@Scheduled`-Jobs. Default ist `true`. Mit `false` startet die Anwendung normal, Cron-Jobs laufen aber nicht automatisch; manuelle Maintenance-Endpunkte bleiben nutzbar.

## Docker

```bash
docker build -t ddbid .
docker run --rm -p 8080:8080 -v ddbid-data:/app/data --env-file .env ddbid
```

Das Image laeuft als eigener User `ddbid`, speichert Daten unter `/app/data` und erwartet Docker-taugliche Variablennamen mit Unterstrichen, z.B. `DDBID_PORT` statt `DDBID.PORT`. Der Scheduler ist im Image absichtlich per `SCHEDULER_ENABLED=false` deaktiviert, damit bestehende Daten beim Containerstart nicht durch einen Importlauf ersetzt werden.

Warum im Dockerfile nicht `spring-boot:run`? Weil das ein Entwicklungsbefehl ist. Das Dockerfile baut im Build-Stage das executable Spring-Boot-Jar und startet im Runtime-Stage nur dieses Jar. Das Runtime-Image bleibt dadurch kleiner, braucht kein Maven, keinen Quellcode und startet deterministischer. Nerd-Faustregel: `spring-boot:run` fuer die Werkbank, `java -jar` fuer den Maschinenraum.

Wichtige Variablen:

```properties
DDBID_PORT=8080
DDBID_DATABASE_TYPE=h2
DDBID_DATABASE_FILE=/app/data/ddbid_duckdb_DO_NOT_DELETE_ITS_IMPORTANT.db
DDBID_DUMP_LOCKFILE=/app/data/DUMP_IS_RUNNING.lock
DDBID_DATAPATH_ITEM=/app/data/dumps/item/
DDBID_DATAPATH_PERSON=/app/data/dumps/person/
DDBID_DATAPATH_ORGANIZATION=/app/data/dumps/organization/
SCHEDULER_ENABLED=false
```

PostgreSQL statt H2:

```properties
DDBID_DATABASE_TYPE=postgres
DDBID_DATABASE_URL=jdbc:postgresql://localhost:5432/ddbid
DDBID_DATABASE_USER=ddbid
DDBID_DATABASE_PASSWORD=change-me
SCHEDULER_ENABLED=false
```

Im PostgreSQL-Modus bleibt `DDBID_DATABASE_FILE` ungenutzt, solange `DDBID_DATABASE_URL` gesetzt ist.

## Bestehende Daten

Beim Start werden keine Tabellen geloescht und fehlende Statistik-Indizes nicht automatisch angelegt. Das ist bewusst langweilig und genau deshalb gut: keine Tabellen-Drops, keine Datenmigration mit Seiteneffekten, kein "ich hab nur mal kurz optimiert" mit Rauch aus dem Serverraum.

Fehlende Indizes koennen gezielt per Maintenance-Endpunkt erstellt werden:

```text
GET /maintenance/indexes
```

Bei H2 auf grossen Datenbanken sollte das in einem Wartungsfenster passieren, weil H2 Tabellen waehrend der Index-Erstellung sperren kann.

## Konfiguration

Die Anwendung liest Konfiguration aus `application.properties`, echten Umgebungsvariablen und aus `src/main/resources/.env` via `springboot4-dotenv`. Die Datei `src/main/resources/.env.example` ist die Vorlage fuer lokale Setups. Geheimnisse gehoeren nicht ins Git, auch wenn sie noch so schoen nach Testdaten aussehen.

| Variable | Default | Beschreibung |
| --- | --- | --- |
| `DDBID_PORT` | `8080` | HTTP-Port der Spring-Boot-Anwendung. |
| `DDBID_PATHPREFIX` | `/` | Servlet Context Path, z.B. `/ddbid` hinter einem Reverse Proxy. |
| `SCHEDULER_ENABLED` | `true` | Schaltet automatische `@Scheduled`-Jobs ein oder aus. Mit `false` startet die UI, Cron-Jobs laufen aber nicht automatisch. |
| `DDBID_CRON_OBJECTS` | `0 0 6 * * FRI` | Cron-Ausdruck fuer den Objekt-Dump/Compare/Import-Ablauf. |
| `DDBID_CRON_RETRY_DELAY` | `600000` | Wartezeit zwischen Retry-Versuchen in Millisekunden. |
| `DDBID_CRON_RETRY_MAX_ATTEMPTS` | `5` | Maximale Anzahl Retry-Versuche. `DDBID_CRON_RETRY_MAXATTEMPTS` wird aus Kompatibilitaetsgruenden ebenfalls noch gelesen. |
| `DDBID_DATABASE_TYPE` | `h2` | Datenbanktyp: `h2`, `postgres`, `postgresql` oder `pg`. |
| `DDBID_DATABASE_FILE` | `data/ddbid_DO_NOT_DELETE_ITS_IMPORTANT.db` | H2-Dateipfad. |
| `DDBID_DATABASE_URL` | leer | JDBC-URL fuer Postgres, z.B. `jdbc:postgresql://localhost:5432/ddbid`. Hat Vorrang vor `DDBID_DATABASE_FILE`. |
| `DDBID_DATABASE_USER` | leer | Datenbankbenutzer fuer Postgres. |
| `DDBID_DATABASE_PASSWORD` | leer | Datenbankpasswort fuer Postgres. |
| `DDBID_DATABASE_INDEXES_AUTO` | `false` | Erstellt fehlende Performance-Indizes beim Start. Bei grossen H2-Dateien besser gezielt ueber `/maintenance/indexes` ausfuehren. |
| `DDBID_DATABASE_QUERY_TIMEOUT_SECONDS` | `300` | Maximale Laufzeit einer einzelnen Datenbankabfrage in Sekunden. |
| `DDBID_CACHE_REFRESH_CRON` | `0 0 */4 * * *` | Cron-Ausdruck fuer den regelmaessigen Entity-Cache-Refresh (Items, Personen, Organisationen). |
| `DDBID_STATISTICS_CACHE_REFRESH_CRON` | `0 0 */4 * * *` | Cron-Ausdruck fuer den regelmaessigen Statistik-Cache-Refresh. |
| `DDBID_SERVICE_LOG_LEVEL` | `INFO` | Log-Level fuer Services. Mit `DEBUG` werden SQL, Parameter und Abfragezeiten geloggt. |
| `DDBID_DUMP_LOCKFILE` | `data/DUMP_IS_RUNNING.lock` | Lock-Datei, die parallele Dump-Laeufe verhindert. |
| `DDBID_DATAPATH_ITEM` | `data/dumps/item/` | Verzeichnis fuer Item-Dumps. |
| `DDBID_DATAPATH_PERSON` | `data/dumps/person/` | Verzeichnis fuer Person-Dumps. |
| `DDBID_DATAPATH_ORGANIZATION` | `data/dumps/organization/` | Verzeichnis fuer Organization-Dumps. |
| `DDBID_SECURITY_USER` | `user` | Benutzername fuer die Web-Oberflaeche. |
| `DDBID_SECURITY_PASSWORD` | `password` | Passwort fuer die Web-Oberflaeche. |

Beispiel fuer H2:

```properties
DDBID_DATABASE_TYPE=h2
DDBID_DATABASE_FILE=data/ddbid_h2_DO_NOT_DELETE_ITS_IMPORTANT.db
DDBID_DATABASE_INDEXES_AUTO=false
SCHEDULER_ENABLED=false
```

Beispiel fuer Postgres:

```properties
DDBID_DATABASE_TYPE=postgres
DDBID_DATABASE_URL=jdbc:postgresql://localhost:5432/ddbid
DDBID_DATABASE_USER=ddbid
DDBID_DATABASE_PASSWORD=change-me
DDBID_DATABASE_INDEXES_AUTO=true
```

Fuer Performance-Diagnose:

```properties
DDBID_SERVICE_LOG_LEVEL=DEBUG
```

Dann erscheinen DataTable- und Statistik-Abfragen inklusive Parametern und Laufzeit im Log.

## Suchspalten und Suchmodus

Jede Spalte in den Tabellen unterstuetzt entweder nur exakte Suche (`=`) oder zusaetzlich Teilstringsuche (`ILIKE '%…%'`). Kein Raten, kein automatisches Umschalten – der Suchmodus ist pro Spalte fest definiert.

| Entitaet | Spalte | Exact | Contains |
|---|---|---|---|
| item | timestamp | ✓ | – |
| item | id | ✓ | – |
| item | status | ✓ | – |
| item | provider\_item\_id | ✓ | ✓ |
| item | dataset\_id | ✓ | – |
| item | label | ✓ | ✓ |
| item | provider\_id | ✓ | ✓ |
| item | sector\_fct | ✓ | – |
| item | supplier\_id | ✓ | – |
| person | timestamp | ✓ | – |
| person | id | ✓ | – |
| person | status | ✓ | – |
| person | variant\_id | ✓ | ✓ |
| person | preferredName | ✓ | ✓ |
| person | type | ✓ | – |
| organization | timestamp | ✓ | – |
| organization | id | ✓ | – |
| organization | status | ✓ | – |
| organization | variant\_id | ✓ | ✓ |
| organization | preferredName | ✓ | ✓ |
| organization | type | ✓ | – |

Fuer Spalten mit Contains-Suche existieren GIN-Trigram-Indizes (`pg_trgm`), damit `ILIKE '%...%'` auch auf grossen Tabellen schnell bleibt. Exact-only-Spalten nutzen ausschliesslich B-Tree-Indizes. Das Suchmodus-Dropdown in der UI zeigt "Contains" nur dann an, wenn die Spalte es unterstuetzt.
