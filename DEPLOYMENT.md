# Déploiement — Panorama IDF

Guide pas-à-pas pour déployer le dashboard sur un VPS Hetzner avec un domaine IONOS.

---

## 1. Acheter le domaine sur IONOS (5 min)

1. Va sur [ionos.fr](https://www.ionos.fr/domaines)
2. Cherche `tdelard.me` (ou un autre nom)
3. Ajoute au panier **uniquement le domaine** (pas d'hébergement)
4. Finalise l'achat (~5€/an le `.fr`)

---

## 2. Créer le VPS Hetzner (10 min)

1. Crée un compte sur [hetzner.com](https://www.hetzner.com/cloud)
2. **Projects** → **New project** → nomme-le `panorama-idf`
3. **Add Server** :
   - **Location** : Falkenstein (Allemagne) ou Helsinki (Finlande)
   - **Image** : Ubuntu 24.04
   - **Type** : **CX22** (4.35€/mo, 2 vCPU, 4GB RAM, 40GB SSD)
   - **Networking** : IPv4 public
   - **SSH keys** : ajoute ta clé publique (`cat ~/.ssh/id_ed25519.pub`). Si tu n'en as pas :
     ```bash
     ssh-keygen -t ed25519 -C "thomas.delard@gmail.com"
     ```
   - **Name** : `panorama-idf-prod`
4. **Create & Buy now**
5. Note l'**IP publique** (ex: `95.217.123.45`)

---

## 3. Pointer le domaine vers le VPS (5 min + propagation DNS)

Sur IONOS :
1. **Mes domaines** → clique sur ton domaine → **DNS**
2. Ajoute ou modifie :
   - Type `A`, Nom `@`, Valeur = **IP du VPS**, TTL 3600
   - Type `A`, Nom `www`, Valeur = **IP du VPS**, TTL 3600
3. Supprime les autres enregistrements A existants
4. Attends 10-30 min. Teste avec :
   ```bash
   dig tdelard.me +short
   # Doit afficher l'IP du VPS
   ```

---

## 4. Durcir le VPS (15 min)

Connexion SSH en root :
```bash
ssh root@IP_DU_VPS
```

### Mettre à jour et créer un user non-root

```bash
apt update && apt upgrade -y
adduser thomas   # choisis un mot de passe, laisse les autres champs vides
usermod -aG sudo thomas
rsync --archive --chown=thomas:thomas ~/.ssh /home/thomas
```

### Installer Docker

```bash
curl -fsSL https://get.docker.com | sh
usermod -aG docker thomas
```

### Firewall

```bash
ufw allow OpenSSH
ufw allow 80
ufw allow 443
ufw enable   # répond y
```

### Désactiver root SSH et mot de passe SSH

```bash
sed -i 's/^#*PermitRootLogin.*/PermitRootLogin no/' /etc/ssh/sshd_config
sed -i 's/^#*PasswordAuthentication.*/PasswordAuthentication no/' /etc/ssh/sshd_config
systemctl restart sshd
```

Déconnecte-toi et reconnecte-toi en `thomas` :
```bash
exit
ssh thomas@IP_DU_VPS
```

---

## 5. Cloner le projet (5 min)

```bash
cd ~
git clone https://github.com/thomasdlr/panorama_idf.git
cd panorama_idf
```

### Installer uv

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
source ~/.bashrc
```

### Configurer les secrets

```bash
cp .env.example .env
# Génère des mots de passe forts
echo "POSTGRES_PASSWORD=$(openssl rand -base64 24)" >> .env
echo "MB_ADMIN_PASSWORD=$(openssl rand -base64 24)" >> .env
nano .env   # édite DOMAIN, EMAIL, vérifie le reste
```

---

## 6. Ingérer les données (30 min)

```bash
uv sync
uv run ingest
```

Ça télécharge ~500 MB de données INSEE / DVF / etc. Prends un café.

---

## 7. Builder les modèles dbt (2 min)

```bash
cd dbt
uv run dbt seed --profiles-dir .
uv run dbt build --profiles-dir .
cd ..
```

---

## 8. Démarrer Docker + Metabase + Caddy (5 min)

```bash
docker compose -f docker-compose.prod.yml --env-file .env up -d
```

Vérifie que tout tourne :
```bash
docker compose -f docker-compose.prod.yml ps
```

Caddy va automatiquement obtenir un certificat HTTPS Let's Encrypt pour ton domaine. Vérifie dans ~30 secondes :
```bash
curl -I https://tdelard.me   # doit répondre 200
```

---

## 9. Créer le dashboard (5 min)

`scripts/setup_metabase.py` lit automatiquement les variables d'environnement (et `.env` à la racine) : `METABASE_URL`, `MB_ADMIN_EMAIL`, `MB_ADMIN_PASSWORD`, `POSTGRES_*`, `COMPOSE_FILE`.

En prod, les ports Metabase (3000) et Postgres (5480) sont bindés sur `127.0.0.1` uniquement — le script tourne sur le VPS et peut donc les joindre via `localhost`, mais ils ne sont pas exposés sur internet.

Indique au script qu'on utilise la stack de prod (sinon il essaiera `docker-compose.yml`) :

```bash
export COMPOSE_FILE=docker-compose.prod.yml
uv run python scripts/setup_metabase.py
```

---

## 10. Rendre le dashboard public (3 min)

1. Ouvre `https://tdelard.me` dans ton navigateur
2. Connecte-toi avec les credentials admin
3. **Settings (engrenage)** → **Admin settings** → **Public sharing** → **Enable**
4. Va sur le dashboard "Panorama Ile-de-France"
5. **Sharing (flèche)** → **Create a public link**
6. Copie le lien public (format `https://tdelard.me/public/dashboard/UUID`)

Tu peux partager ce lien, personne n'a besoin de compte pour consulter.

---

## 11. Analytics visiteurs (optionnel, 5 min)

### Option simple : Cloudflare proxy

1. Crée un compte [cloudflare.com](https://cloudflare.com)
2. **Add site** → `tdelard.me`
3. Cloudflare te donne 2 nameservers. Retourne sur IONOS :
   - **DNS** → **Name servers** → remplace par ceux de Cloudflare
4. Attends la propagation (10 min à 24h)
5. Dans Cloudflare :
   - **DNS** → active le **proxy (orange cloud)** sur `@` et `www`
   - **SSL/TLS** → mode **Full (strict)**
   - **Analytics** → visites uniques, pays, bots, etc.

### Option plus fine : GoAccess sur les logs Caddy

```bash
ssh thomas@IP_DU_VPS
sudo apt install goaccess -y

# Génère un rapport HTML depuis les logs Caddy
docker exec panorama_idf_caddy cat /data/access.log | \
  goaccess - --log-format=CADDY -o /tmp/stats.html
```

---

## Maintenance

### Mettre à jour le dashboard
```bash
cd ~/panorama_idf
git pull
set -a && source .env && set +a
cd dbt && uv run dbt build --profiles-dir . && cd ..
uv run python scripts/setup_metabase.py
```

### Redémarrer les services
```bash
docker compose -f docker-compose.prod.yml restart
```

### Voir les logs
```bash
docker compose -f docker-compose.prod.yml logs -f metabase
docker compose -f docker-compose.prod.yml logs -f caddy
```

### Backup DuckDB + PostgreSQL

Un script `scripts/backup.sh` automatise tout (DuckDB + PG marts + PG Metabase appdb, gzippés, rotation 7 jours) :

```bash
./scripts/backup.sh
# → dumps dans ~/backups/{duckdb,pg-marts,pg-metabase}-YYYY-MM-DD.{duckdb.gz,sql.gz}
```

Planifier en cron (tous les jours à 3h du matin) :

```bash
crontab -e
# Ajouter :
0 3 * * * cd /home/thomas/panorama_idf && ./scripts/backup.sh >> logs/backup.log 2>&1
```

Rapatrier régulièrement en local ou pousser vers un bucket (Backblaze B2, ~0.5 €/mois pour des dumps gzippés) :

```bash
# Rapatriement manuel
scp thomas@IP:~/backups/* ./local-backups/

# Ou sync auto vers B2 (ajouter un second cron)
# 0 4 * * * rclone sync ~/backups b2:mon-bucket-backups --min-age 1h
```

---

## Coûts récurrents

- Hetzner CX22 : **4.35€/mois** (~52€/an)
- Domaine `.fr` IONOS : **~5-8€/an** après la 1ère année promo
- Cloudflare : **0€** (tier gratuit)

**Total : ~60€/an**.
