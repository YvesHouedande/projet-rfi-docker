# Pipeline d'ingestion des flux RSS de RFI

Les flux RSS de RFI sont récupérés en CSV, chargés dans PostgreSQL et orchestrés par
**Apache Airflow 3** (3.3.2) via **Docker Compose**. Les tâches d'ingestion tournent dans des
conteneurs éphémères lancés par le `DockerOperator`.

## Architecture

```
 Flux RSS RFI ──► docker compose (réseau "rfi-net")
   Airflow 3 : api-server, scheduler, dag-processor
    ├─ DAG rfi_pipeline (@hourly)
    │    get_feeds ─► fetch_feed[0..n] (mappé, parallèle) ─► build_load_command
    │                                   ─► load_to_postgres ─► purge_old_csv
    └─ DAG rfi_publish_image (manuel) : build + push de l'image sur Docker Hub
   Volume Docker nommé "rfi-data" monté sur /data (un seul dossier, pas de raw/archive)
   PostgreSQL "rfi-postgres" (données RFI) ; SonarQube (profil compose "quality")
```

- `airflow-db` : métadonnées d'Airflow ; `rfi-postgres` : table `rfi_articles` (port publié
  sur `127.0.0.1` uniquement, 5433 par défaut).
- Airflow pilote le démon Docker **de l'hôte** via `/var/run/docker.sock` : le volume
  `rfi-data` et le réseau `rfi-net` sont donc résolus côté hôte.
- Code d'ingestion : `src/rfi_ingest/fetch.py` (RSS → CSV) et `load.py` (CSV → PostgreSQL),
  packagés dans l'image `rfi-ingest` (`Dockerfile`).

## Principe directeur : l'idempotence

Une tâche relancée avec le même contexte (retry, « Clear » dans l'UI) aboutit au même état
final, sans doublon ni erreur :

1. **Nom de fichier déterministe** : `/data/<slug-du-flux>__<run-tag>.csv`, le run-tag étant
   le `run_id` Airflow nettoyé. Une relance écrase le même fichier.
2. **Écriture atomique** : fichier `.csv.tmp` puis renommage.
3. **Le chargement ne déplace ni ne supprime aucun CSV** et reçoit une **liste de chemins
   explicite** (jamais de balayage de dossier).
4. **Une seule transaction** (tout ou rien) avec `ON CONFLICT (link) DO NOTHING`. Les
   fichiers sont lus avant d'ouvrir la transaction.
5. `CREATE TABLE IF NOT EXISTS` est exécuté dans cette même transaction (une seule tâche de
   chargement : pas de course entre DDL parallèles).

**Limite** : un flux RSS est une fenêtre glissante. Rejouer un ancien run récupère les
articles *du moment*, enregistrés sous le nom de fichier de l'ancien run. La garantie porte
sur l'absence de doublon et la convergence de l'état, pas sur la reconstitution exacte du
contenu de l'époque.

Le DAG `rfi_pipeline` ne passe pas en échec si seuls certains flux échouent : la tâche
`purge_old_csv` (`trigger_rule="all_done"`) est la dernière et réussit, donc le run est
marqué réussi. Les instances `fetch_feed` en échec restent visibles (rouges) dans l'UI.

## Prérequis

- Linux ou WSL2, Docker Engine ou Docker Desktop avec le plugin `docker compose`.
- `make`, `python3` (pour `make init`) ; Python 3.12 pour lancer les tests en local.

## Démarrage rapide

```bash
make init     # crée .env, renseigne uid / groupe docker / chemin, génère les secrets
make build    # image rfi-ingest:latest + images compose
make up       # démarre Airflow et PostgreSQL
```

Interface : <http://localhost:8080> — identifiants `admin / admin` (simple auth manager,
mot de passe fixe défini dans `config/simple_auth_passwords.json`).

Activez le DAG `rfi_pipeline`, puis déclenchez un run. **Contrôlez les URLs des flux au
premier run** : elles sont celles fournies avec le projet et n'ont pas été vérifiées ; un
flux illisible fait échouer sa tâche (elle est retentée 2 fois, 2 minutes d'intervalle).

Vérification des données : `make psql` puis `SELECT count(*) FROM rfi_articles;`.

## Variables Airflow

Définies depuis `.env` par des variables d'environnement `AIRFLOW_VAR_*` (voir
`docker-compose.yml`). Les noms contenant `password`, `secret` ou `token` sont masqués par
Airflow. **Une Variable définie par variable d'environnement prend le pas sur la même
Variable créée dans l'UI.**

| Variable Airflow | Clé `.env` | Rôle | Lecture |
|---|---|---|---|
| `rfi_feeds` | `RFI_FEEDS` | Liste JSON des URLs de flux | exécution |
| `rfi_ingest_image` | `RFI_INGEST_IMAGE` | Image d'ingestion | exécution (templaté) |
| `rfi_data_volume` | `RFI_DATA_VOLUME` | Volume monté sur `/data` | parsing |
| `rfi_docker_network` | `RFI_DOCKER_NETWORK` | Réseau des conteneurs | parsing |
| `rfi_fetch_concurrency` | `RFI_FETCH_CONCURRENCY` | Fetch simultanés (`max_active_tis_per_dag`) | parsing |
| `rfi_postgres_host` | (fixe : `rfi-postgres`) | Hôte PostgreSQL des données | exécution (templaté) |
| `rfi_postgres_db` / `_user` / `_password` | `RFI_POSTGRES_*` | Accès PostgreSQL | exécution (templaté) |
| `rfi_retention_days` | `RFI_RETENTION_DAYS` | Rétention des CSV (jours) | exécution (templaté) |
| `dockerhub_user` / `dockerhub_token` | `DOCKERHUB_*` | Identifiants Docker Hub | exécution (templaté) |
| `rfi_image_tag` | `RFI_IMAGE_TAG` | Tag publié sur Docker Hub | exécution (templaté) |
| `rfi_project_dir` | `RFI_PROJECT_DIR` | Chemin du projet **côté hôte** | parsing |

Champs templatés du `DockerOperator` (provider 4.5.x) : `image`, `command`, `environment`,
`env_file`, `container_name`, `mounts`.

## Configuration

- **Liste des flux** : modifiez `RFI_FEEDS` dans `.env` (JSON entre apostrophes), puis
  `docker compose up -d` ; ou supprimez la variable d'environnement et créez la Variable
  `rfi_feeds` dans l'UI.
- **Fréquence** : `schedule="@hourly"` dans `dags/rfi_pipeline.py` (expression cron
  possible, ex. `"*/30 * * * *"`).
- **Concurrence** : `RFI_FETCH_CONCURRENCY`. **Rétention** : `RFI_RETENTION_DAYS`.

## Publication sur Docker Hub

Renseignez `DOCKERHUB_USER`, `DOCKERHUB_TOKEN` (jeton d'accès, pas le mot de passe du
compte) et `RFI_IMAGE_TAG` dans `.env`, relancez `make up`, puis déclenchez à la main le DAG
`rfi_publish_image`. Il lance `docker:cli` avec le socket Docker et le projet monté en
lecture seule sur `/src` : `docker login --password-stdin`, `docker build`, `docker push`,
`docker logout`. `RFI_PROJECT_DIR` doit être un chemin **de l'hôte** (rempli par `make init`).

## Qualité : SonarQube

```bash
make sonar-up                    # http://localhost:9000 (admin / admin, à changer)
# Créez un token : « Mon compte > Sécurité », puis :
export SONAR_TOKEN=...
make sonar                       # lance pytest (coverage.xml) puis sonar-scanner
```

Sous WSL, SonarQube (Elasticsearch) peut exiger : `sudo sysctl -w vm.max_map_count=262144`.
La configuration est dans `sonar-project.properties`. `make install` puis `make test`
lancent les tests seuls (`pytest --cov`).

## Tests

- `tests/test_fetch.py`, `tests/test_load.py` : sans réseau ni base (flux RSS fourni en
  chaîne, fausse connexion PostgreSQL).
- `tests/test_dags.py` : import des DAGs via `DagBag`. **Ignoré automatiquement si Airflow
  n'est pas installé** dans l'environnement local (`pytest.importorskip`).

## Sécurité

- Le socket Docker monté dans les conteneurs Airflow donne un **accès root à l'hôte** :
  acceptable pour un usage local, **pas en production**.
- Ne jamais committer `.env` (secrets, jeton Docker Hub) : il est dans `.gitignore`.
- Le mot de passe `admin / admin` et les mots de passe de bases du `.env.example` sont des
  valeurs de démonstration à changer hors poste local.

## Dépannage

| Symptôme | Cause probable / action |
|---|---|
| `permission denied` sur `/var/run/docker.sock` | Groupe du socket non pris en compte : relancer `make init` (met à jour `DOCKER_GID`) puis `make up` |
| `Permission denied` sur `/data` | Le volume `rfi-data` a été créé avec de mauvais droits : l'arrêter puis `docker volume rm rfi-data` |
| DAG absent de l'UI | Consulter les logs du `dag-processor` : `docker compose logs airflow-dag-processor` |
| Échec du build / push Docker Hub | Vérifier `RFI_PROJECT_DIR` (chemin hôte), `DOCKERHUB_USER` et `DOCKERHUB_TOKEN` |
| `network rfi-net not found` | Démarrer d'abord la pile : `make up` (le réseau s'appelle exactement `rfi-net`) |
| Image `rfi-ingest:latest` introuvable | Lancer `make build` |

## Limites connues

Ce qui a été vérifié lors de la livraison : `pytest --cov` (tests unitaires), compilation de
tous les `.py`, validité du `docker-compose.yml` (`docker compose config -q`), `make init`
dans une copie du projet, et import des DAGs avec Airflow 3.3.2 / provider Docker 4.5.9 dans
un venv jetable (`DagBag`, sans erreur d'import).

**Non testé** :

- build des images (`Dockerfile`, `Dockerfile.airflow`) : aucun démon Docker n'était
  disponible, et l'existence du tag `apache/airflow:3.3.2` n'a pas été contrôlée ;
- démarrage réel d'Airflow (api-server, scheduler, dag-processor, healthcheck) ;
- exécution réelle des DAGs, y compris le comportement du XCom d'une tâche mappée dont
  certaines instances échouent (le code filtre les valeurs vides ou `None`) ;
- build et push Docker Hub ;
- accès réel aux flux RSS de RFI (URLs non vérifiées) ;
- insertion réelle en base PostgreSQL (la transaction est testée avec des doubles).

Autres limites : `safe_tag` remplace les caractères non autorisés par un tiret au lieu de les
supprimer (pour éviter les collisions de noms de fichiers) ; le `max_active_tis_per_dag` est
lu au parsing, un changement demande de recharger le DAG.
