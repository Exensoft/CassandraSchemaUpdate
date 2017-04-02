# Cassandra Schema Update

[![Build Status](https://travis-ci.org/Exensoft/CassandraSchemaUpdate.svg?branch=master)](https://travis-ci.org/Exensoft/CassandraSchemaUpdate)
[![Coverage Status](https://coveralls.io/repos/github/Exensoft/CassandraSchemaUpdate/badge.svg?branch=master)](https://coveralls.io/github/Exensoft/CassandraSchemaUpdate?branch=master)

1. [Présentation](#presentation)
2. [Fonctionnement](#fonctionnement)
	1. [Décrire les éléments](#décrire-les-éléments)
		1. [Décrire une colonne](#décrire-une-colonne)
		2. [Décrire une table](#décrire-une-table)
		3. Décrire un keyspace
	2. Créer un patch et l'exécuter

## Présentation
Cette librairie permet de réaliser simplement les actions suivantes sur une base de données Cassandra :

 * Obtenir la liste des keyspaces du cluster
 * Obtenir une description des tables d'un keyspace
 * Créer un keyspace avec ses tables
 * Mettre à jour un schéma existant en conservant au maximum les données


## Fonctionnement

### Décrire les éléments
Pour utiliser CassandraSchemaUpdate, il faut dans un premier temps savoir décrire le schéma que l'on veut créer. 
Un schéma se compose principalement des éléments suivants : `Keyspace`, `Table`et `Column`. Nous allons voir comment décrire ces structures.

#### Décrire une colonne
Une colonne est décrite par son nom et son type. Pour en construire une, il faut créer une instance de la classe `Column`. Voici comment construire une colonne simple :
```java
Column column = new Column("name", BasicType.INT);
```

Les types basiques disponibles sont les suivants : 
| Type Cassandra   |    Constante associée |
| ---------------- | --------------------- |
| `ascii`          | `BasicType.ASCII`     |
| `bigint`         | `BasicType.BIGINT`    |
| `blob`           | `BasicType.BLOB`      |
| `boolean`        | `BasicType.BOOLEAN`   |
| `counter`        | `BasicType.COUNTER`   |
| `decimal`        | `BasicType.DECIMAL`   |
| `double`         | `BasicType.DOUBLE`    |
| `float`          | `BasicType.FLOAT`     |
| `inet`           | `BasicType.INET`      |
| `int`            | `BasicType.INT`       |
| `text`           | `BasicType.TEXT`      |
| `timestamp`      | `BasicType.TIMESTAMP` |
| `timeuuid`       | `BasicType.TIMEUUID`  |
| `uuid`           | `BasicType.UUID`      |
| `varchar`        | `BasicType.VARCHAR`   |
| `varint`         | `BasicType.VARINT`    |

En plus de ces types basiques, il est possibles d'utiliser des structures plus complexes telles que de *map*, des *list* ou des *set*.

Les *list* et les *set* sont des ensembles d'éléments d'un autre type, ils se déclarent de manière analogue.
Il faut construire des instances de `ListType`ou de `SetType`avec le type fils en paramètre :

```java
// Création d'une colonne de type list<int>
Column column1 = new Column("colum1", new ListType(BasicType.INT));

// Création d'une colonne de type set<text>
Column column2 = new Column("colum2", new SetType(BasicType.TEXT));
```

Les *map* sont des ensembles de clé d'un premier type et de valeurs d'un second type.
Pour créer une map, il faut utiliser une instance de `MapType`, on lui donne en premier paramètre le type de la clé et en second paramètre le type des valeurs :
```java
//Création d'une colonne de type map<int, text>
Column column = new Column("column", new MapType(BasicType.INT, BasicType.TEXT));
```

#### Décrire une table
Pour décrire une table il faut créer une instance de la classe `Table` avec comme paramètre le nom de la table :
```java
Table table = new Table("table_name");
```

La méthode `addColumn` permet d'ajouter une colonne à la table, il prend en paramètre une instance de la classe `Column` qui permet de décrire la colonne à ajouter. `Column` prend en paramètre le nom de la colonne ainsi que son type.
```java
table.addColumn(new Column("column_name", BasicType.VARCHAR));
```

Pour définir une clé primaire il faut utiliser la méthode `addPartitioningKey`  :
```java
table.addPartitioningKey("column_name");
```

Voici comment créer une table simple :

```java
/**
	Table à créer : 
  CREATE TABLE users (
    user_name varchar PRIMARY KEY,
    password varchar,
    gender varchar,
    session_token varchar,
    state varchar,
    birth_year bigint
  );
*/

Table table = new Table("users")
      .addColumn(new Column("user_name", BasicType.VARCHAR))
      .addColumn(new Column("password", BasicType.VARCHAR))
      .addColumn(new Column("gender", BasicType.VARCHAR))
      .addColumn(new Column("session_token", BasicType.VARCHAR))
      .addColumn(new Column("state", BasicType.VARCHAR))
      .addColumn(new Column("birth_year", BasicType.VARCHAR))
      .addColumn(new Column("state", BasicType.VARCHAR))
      .addPartitioningKey("user_name");
```
