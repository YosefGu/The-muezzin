docker run -d `
--name my-mongodb-muezzin `
--network muezzin-net `
-p 27017:27017 `
mongodb/mongodb-community-server:latest