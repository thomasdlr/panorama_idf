FROM nginx:alpine
COPY nginx.conf /etc/nginx/conf.d/default.conf
EXPOSE 80
# Les GeoJSON sont écrits par le service `pipeline` dans le volume
# monté sur /usr/share/nginx/html (./data/metabase côté hôte).
