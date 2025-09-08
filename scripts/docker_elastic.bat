docker run -d --name elasticsearch-muezzin `
--network muezzin-net `
-p 9200:9200 `
-e "discovery.type=single-node" `
-e "xpack.security.enabled=false" `
-e "ES_JAVA_OPTS=-Xms1g -Xmx1g" `
docker.elastic.co/elasticsearch/elasticsearch:8.15.0