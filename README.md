# Cassandra Schema Update

1. [Présentation](#presentation)
2. [Fonctionnement](#fonctionnement)

## Présentation
Cette librairie permet de réaliser simplement les actions suivantes sur une base de données Cassandra :

 * Obtenir la liste des keyspaces du cluster
 * Obtenir une description des tables d'un keyspace
 * Créer un keyspace avec ses tables
 * Mettre à jour un schéma existant en conservant au maximum les données


## Fonctionnement

### Décrire une table
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
