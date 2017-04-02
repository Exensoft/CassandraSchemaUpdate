# Cassandra Schema Update

[![Build Status](https://travis-ci.org/Exensoft/CassandraSchemaUpdate.svg?branch=master)](https://travis-ci.org/Exensoft/CassandraSchemaUpdate)
[![Coverage Status](https://coveralls.io/repos/github/Exensoft/CassandraSchemaUpdate/badge.svg?branch=master)](https://coveralls.io/github/Exensoft/CassandraSchemaUpdate?branch=master)

1. [Présentation](#presentation)
2. [Fonctionnement](#fonctionnement)
	1. [Décrire les éléments](#décrire-les-éléments)
		1. [Décrire une colonne](#décrire-une-colonne)
		2. [Décrire une table](#décrire-une-table)
		3. [Décrire un keyspace](#décrire-un-keyspace)
	2. [Créer un patch et l'exécuter](#créer-un-patch-et-lexécuter)
		1. [Créer un patch](#créer-un-patch)
		2. [Explorer un patch](#explorer-un-patch)
		3. [Exécuter un patch](#exécuter-un-patch)

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

Table table = new Table("users")
      .addColumn(new Column("user_name", BasicType.VARCHAR))
      .addColumn(new Column("password", BasicType.VARCHAR))
      .addColumn(new Column("mail", BasicType.VARCHAR))
      .addPartitioningKey("user_name");
```


Il est également possible de définir une clustering colum. Il faut utiliser la méthode `addClusteringColumn` :
```java
table.addClusteringColumn("column_name");
```
Par défaut, les valeurs d'une clustering column sont triées par ordre croissant. Si vous souhaitez passer à un ordre décroissant vous pouvez le préciser lors de la déclaration de la clustering column :
```java
table.addClusteringColumn("column_name", SortOrder.DESC)
```

Il est également possible de déclarer un index sur une colonne qui ne fait pas partie de la partitionning key ni des clustering columns.
Il faut utiliser la méthode `addIndex`, elle prend en premier paramètre le nom de l'index que vous souhaitez créer en second paramètre le nom de la colonne sur laquelle l'index doit être placé.
```java
table.addIndex("index_name", "column_name");
```

#### Décrire un Keyspace

Le Keyspace va contenir les différentes tables de votre schéma. Pour le décrire, il faut utiliser la classe `Keyspace` en spécifiant le nom que vous souhaitez donner à votre keyspace en paramètre :

```java
Keyspace keyspace = new Keyspace("keyspace_name");
```

Vous pouvez ensuite ajouter des tables à votre keyspace :
```java
keyspace.addTable(table);
```
### Créer un patch et l'exécuter

Classiquement, CassandraSchemaUpdate fonctionne en trois grande étapes :

 * Description de votre schéma (une instance de `Keyspace`)
 * Comparaison de votre schéma avec le schéma actuellement existant dans Cassandra, création d'un patch décrivant la différence entre les deux schémas (le delta)
 * Application du patch créé à l'étape précédente

Nous avons choisi de séparer la création du patch de son application car il peut arriver que certains patch entraînent une perte de données. Nous avons donc préféré mettre à disposition des méthodes permettant d'explorer le patch et de vérifier où se situent les pertes de données si il y en a.

#### Créer un patch
Pour créer un patch, il faut dans un premier temps créer une instance de `SchemaUpdate`.

On commence par configurer une connexion à Cassandra :
```java
Cluster cluster = Cluster.builder()
		.withPort(9042)
		.addContactPoint("localhost")
		.build();
```
Et on crée ensuite notre instance :
```java
SchemaUpdate schemaUpdate = new SchemaUpdate.Builder()
		.withCluster(cluster)
		.build();
```

Pour créer un patch il faut que vous ayez au préalable définit le `Keyspace` cible que vous souhaitez obtenir. Il suffit ensuite d'appeler la méthode `createPatch` :

```java
DeltaResult patch = schemaUpdate.createPatch(targetKeyspace);
```
L'objet `DeltaResult` retourné contient le patch à exécuter pour obtenir le keyspace cible dans votre base Cassandra.

#### Explorer un patch
Il est possible d'obtenir des informations sur le patch qui a été créé par le `SchemaUpdate`, par exemple on peut vérifier si le patch contient des opérations à effectuer ou si le schéma existant correspond déjà au schéma cible :
```java
//On vérifie si on a des mises à jour à effectuer
if(patch.hasUpdate()) {
	System.out.println("Le patch contient des mises à jour");
}
```

On peut aussi savoir simplement si le patch contient des opérations qui risquent d'engendrer une perte de données :
```java
if(patch.hasFlag(DeltaFlag.DATA_LOSS)) {
	System.out.println("Attention, l'exécution de ce patch risque d'engendrer une perte de données");
}
```

Il est également possible d'aller plus loin dans l'exploration des modifications décrites dans le patch.
L'objet `DeltaResult`est organisé en deux grandes parties : 

 * Description des modifications sur le keyspace
 * Description des modifications sur les différentes tables

Les modifications sur les différentes structures sont modélisées sous forme de `DeltaList`, elles contiennent les opérations "élémentaires" (1 opération = 1 requête CQL) à exécuter sur le cluster pour obtenir le schéma cible.

On peut récupérer la DeltaList des opérations à exécuter sur le keyspace de la manière suivante :
```java
DeltaList keyspaceDelta = patch.getKeyspaceDelta();
```

On peut récupérer la liste des opérations à exécuter sur une table de la manière suivante :
```java
DeltaList tableDelta = patch.getTablesDelta().get("table_name");
```

Les `DeltaList` permettent l'utilisation des méthode `hasUpdate` et `hasFlag` de la même manière que les `DeltaResult` :
```java
DeltaList tableDelta = patch.getTablesDelta().get("table_name");

if(tableDelta.hasUpdate()) {
	System.out.println("La table table_name doit être mise à jour");
}

if(tableDelta.hasFlag(DeltaFlag.DATA_LOSS)) {
	System.out.println("La mise à jour de la table table_name va engendrer une perte de données");
}
```

#### Exécuter un patch
Lorsque vous avez vérifié si le patch généré peut être exécuté sans crainte pour votre application, vous pouvez l'appliquer en utilisant la méthode `applyPatch` :

```java
schemaUpdate.applyPatch(patch);
```

Après avoir mis à jour votre schéma, il ne faut pas oublier de fermer la connexion :
```java
schemaUpdate.close();
```

## Exemples

### Création d'un schéma
```java
// Création du schéma cible
Keyspace keyspace = new Keyspace("my_application")
		.addTable(new Table("users")
				.addColumn(new Column("login", BasicType.VARCHAR))
				.addColumn(new Column("password", BasicType.VARCHAR))
				.addColumn(new Column("mail", BasicType.VARCHAR))
				.addPartitioningKey("login"))
		.addTable(new Table("messages")
				.addColumn(new Column("id", BasicType.UUID))
				.addColumn(new Column("user1_login", BasicType.VARCHAR))
				.addColumn(new Column("user2_login", BasicType.VARCHAR))
				.addColumn(new Column("date", BasicType.VARCHAR))
				.addColumn(new Column("content", BasicType.VARCHAR))
				.addPartitioningKey("id")
				.addIndex("messages_user1_login_index", "user1_login")
				.addIndex("messages_user2_login_index", "user2_login"));

// Instanciation de notre SchemaUpdate
SchemaUpdate schemaUpdate = new SchemaUpdate.Builder()
		.withCluster(new Cluster.Builder()
				.withPort(9042)
				.addContactPoint("localhost")
				.build())
		.build();

// Création du patch
DeltaResult patch = schemaUpdate.createPatch(keyspace);

// Application du patch
schemaUpdate.applyPatch(patch);

// Fermeture du SchemaUpdate
schemaUpdate.close();
```