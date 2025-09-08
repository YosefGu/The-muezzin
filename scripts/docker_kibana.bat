docker run -d `
  --name kibana-muezzin `
  --network muezzin-net `
  -p 5601:5601 `
  -e ELASTICSEARCH_HOSTS=http://elasticsearch-muezzin:9200 `
  docker.elastic.co/kibana/kibana:8.13.4