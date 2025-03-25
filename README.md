Java Wikipedia dump importer

## Prerequisites

* At least 100GB of disk space
* A Java JRE
* A Postgres database

## Instructions

#### Download the dump:

```shell
wget "https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles.xml.bz2"
```

#### Set up a Postgres database and user.<br>

```shell
sudo apt-get install postgresql
```

or

```shell
docker create --restart=no --name postgres -p5432:5432 -e POSTGRES_PASSWORD=mysecretpassword -v /opt/wikipedia/pg:/var/lib/postgresql/data postgres:15-bookworm
docker exec -it postgres bash
```

#### Create the user and the database

```postgresql
CREATE USER wiki ENCRYPTED PASSWORD 'wiki';
CREATE DATABASE wiki OWNER wiki ENCODING 'UTF-8';
```

#### Create the table as the user

```shell
psql wiki wiki
```

```postgresql
CREATE UNLOGGED TABLE pages
(
    title varchar NOT NULL PRIMARY KEY,
    text  varchar NOT NULL
);
```

UNLOGGED makes it much faster, at the expense of crash-safety.<br>
You can add indexes after the import has completed.

#### Compile the importer

```shell
./mvnw package
```

#### Run the importer:

```shell
java -jar wikidump-importer-1.0-SNAPSHOT.jar enwiki-latest-pages-articles.xml.bz2 jdbc:postgresql://127.0.0.1:5432/wiki wiki wiki
```

#### Wait a long time.
