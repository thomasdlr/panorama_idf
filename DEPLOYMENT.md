# Déploiement — Panorama IDF avec Dokploy

Guide pas-à-pas pour déployer la stack (Metabase + landing page) sur un VPS Hetzner, domaine `tdelard.me` (IONOS), orchestration via [Dokploy](https://dokploy.com). Dokploy remplace Caddy et gère Traefik + Let's Encrypt + deploy auto sur `git push`.

---

## 1. Pré-requis

- VPS Ubuntu 24.04 (Hetzner CX22 recommandé, 4.35 €/mois, 4 Go RAM).
- Domaine `tdelard.me` sur IONOS (ou autre registrar).
- Accès SSH en user non-root avec sudo (voir ancienne procédure Hetzner : `ssh-keygen`, `ufw`, disable root SSH).

---

## 2. DNS (IONOS)

Pointe 3 entrées vers l'IP du VPS :

| Type | Nom       | Valeur        |
|------|-----------|---------------|
| A    | `@`       | IP du VPS     |
| A    | `www`     | IP du VPS     |
| A    | `metabase`| IP du VPS     |

Vérification :
```bash
dig tdelard.me metabase.tdelard.me +short   # les deux doivent répondre avec l'IP du VPS
```

---

## 3. Installer Dokploy sur le VPS

Sur le VPS (user avec sudo + Docker déjà installé) :

```bash
curl -sSL https://dokploy.com/install.sh | sh
```

L'installation prend ~2 min. Dokploy écoute sur le port **3000** par défaut pour son UI.

Ouvre `http://IP_DU_VPS:3000`, crée le compte admin (email + mot de passe fort).

⚠ **Collision de port** : Metabase utilisait aussi le 3000. Dans notre `docker-compose.yml`, Metabase est bindé sur `127.0.0.1:3000` **uniquement** (loopback) et exposé publiquement via Traefik sur `metabase.tdelard.me`. Dokploy et Metabase cohabitent sans conflit.

Pour exposer l'UI Dokploy en HTTPS plus tard : configure `dokploy.tdelard.me` dans Dokploy lui-même (Settings → Server) ; optionnel.

---

## 4. Créer l'application dans Dokploy

### 4.1 Project + Git source

1. Dokploy UI → **Projects** → **Create project** → `panorama-idf`.
2. Dans le project → **Create Service** → **Application** → type **Docker Compose**.
3. **Source** : GitHub (connecte ton compte) → repo `thomasdlr/panorama_idf` → branche `main`.
4. **Compose path** : `docker-compose.yml` (défaut).

### 4.2 Variables d'environnement

**Environment** → paste :
```
METABASE_SITE_URL=https://metabase.tdelard.me
POSTGRES_DB=panorama_idf
POSTGRES_USER=metabase
POSTGRES_PASSWORD=<openssl rand -base64 24>
MB_ADMIN_EMAIL=admin@tdelard.me
MB_ADMIN_PASSWORD=<openssl rand -base64 24>
```

### 4.3 Domaines (Traefik)

**Domains** → ajoute 2 entrées :

| Service    | Host                    | Port | HTTPS | Path |
|------------|-------------------------|------|-------|------|
| `landing`  | `tdelard.me`            | 80   | ✓     | `/`  |
| `landing`  | `www.tdelard.me`        | 80   | ✓     | `/`  |
| `metabase` | `metabase.tdelard.me`   | 3000 | ✓     | `/`  |

Dokploy génère automatiquement les certs Let's Encrypt.

### 4.4 Premier déploiement

**Deploy** → Dokploy clone le repo, build les images (`landing` + `pipeline`), démarre `postgres`, `metabase`, `geojson`, `landing`. Durée : ~3 min (download images + build).

À ce stade `tdelard.me` affiche la landing, mais `metabase.tdelard.me` montre un Metabase vierge (pas encore de données).

---

## 5. Premier run du pipeline (ingest + dbt + dashboard)

Le service `pipeline` n'est pas démarré automatiquement (profile `pipeline`). Il faut le lancer une première fois à la main pour ingérer les données et créer le dashboard.

SSH sur le VPS :

```bash
# Dokploy clone les repos dans /etc/dokploy/compose/<project-id>/code/
cd /etc/dokploy/compose/panorama-idf-<hash>/code
docker compose --profile pipeline run --rm pipeline
```

Durée : **~30-40 min** (téléchargement ~500 MB de données INSEE/DVF, dbt build, setup Metabase).

Tu peux suivre en live. À la fin tu verras :
```
✓ Pipeline terminé
```

Vérifie `https://metabase.tdelard.me/dashboard/6` — dashboard complet avec 3 onglets.

---

## 6. Rendre le dashboard public

Le script force `MB_ENABLE_PUBLIC_SHARING=true` dès le premier démarrage, donc :

1. `https://metabase.tdelard.me` → login admin.
2. Ouvre le dashboard **Panorama Ile-de-France**.
3. **Sharing (icône flèche)** → **Create a public link** → copie l'URL.

L'UUID public dans `landing/index.html` est déjà câblé sur ton dashboard existant (`10c1915f-eb39-42a6-b630-e708ec58f147`). Si tu crées un nouveau dashboard public (UUID différent), **édite `landing/index.html`** et push — Dokploy redéploie tout seul.

---

## 7. Refresh mensuel automatique (Dokploy Schedules)

Les données INSEE / DVF / Filosofi ne sont publiées qu'une fois par an ou au mieux trimestriellement, mais les sources Vélib / pistes cyclables sont temps réel. Un refresh mensuel suffit largement.

Dans Dokploy : **Project → panorama-idf → Schedules → Create Schedule**.

| Champ       | Valeur                                              |
|-------------|-----------------------------------------------------|
| Name        | Refresh mensuel                                     |
| Schedule    | `0 3 1 * *` (le 1er de chaque mois à 3h)            |
| Command     | `docker compose --profile pipeline run --rm pipeline` |
| Working dir | Racine du repo cloné par Dokploy                    |

Si Dokploy ne propose pas de "working dir", utilise la forme absolue :
```
cd /etc/dokploy/compose/panorama-idf-<hash>/code && docker compose --profile pipeline run --rm pipeline
```

---

## 8. Deploy automatique sur `git push`

Dokploy → **Project → Webhook**. Copie l'URL et ajoute-la comme webhook GitHub (`Settings → Webhooks`) avec content-type `application/json` et événement `push`. Désormais chaque push sur `main` → redeploy auto (postgres, metabase, geojson, landing restart ; pipeline non touché, données préservées dans `./data/`).

⚠ Le dossier `data/` est un **bind mount** (`./data:/app/data`). Les données DuckDB persistent entre deploys. Si tu changes la structure du repo, pense à sauvegarder `data/panorama_idf.duckdb` avant d'expérimenter.

---

## 9. Backup

Un script `scripts/backup.sh` peut dumper `data/panorama_idf.duckdb` + les DB postgres. Lance-le en cron sur le VPS (hors Dokploy) :

```bash
crontab -e
0 4 * * * cd /etc/dokploy/compose/panorama-idf-<hash>/code && ./scripts/backup.sh >> /var/log/panorama-backup.log 2>&1
```

Rapatrie les backups vers Backblaze B2 ou un autre bucket.

---

## 10. Dev local (sans Dokploy)

Identique à avant : `docker compose up -d` (sans `--profile pipeline`) démarre postgres/metabase/geojson/landing. Les ports `3000` (metabase) et `5480` (postgres) sont bindés sur `127.0.0.1` donc accessibles en local.

Pour lancer le pipeline en local :
```bash
docker compose --profile pipeline run --rm pipeline
# OU en natif (plus rapide en dev) :
uv run ingest && cd dbt && uv run dbt build --profiles-dir . && cd .. && uv run python scripts/setup_metabase.py
```

Landing page en local : `http://localhost:8080` si tu ajoutes `ports: ["8080:80"]` au service `landing` (pas activé par défaut pour éviter la collision avec Dokploy).

---

## Coûts récurrents

- Hetzner CX22 : **4.35 €/mois**
- Domaine `.me` IONOS : **~15 €/an**
- Dokploy : gratuit (self-hosted)

**Total : ~80 €/an.**
