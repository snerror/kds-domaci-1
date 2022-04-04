# KDS Domaci 1

## Scenario 1

```
cr 1
fi data/disk1/ A 1
start
take wiki-1.txt-arity1
take wiki-2.txt-arity1
merge wiki-1.txt-arity1,wiki-2.txt-arity1
take m-wiki-1.txt-arity1-wiki-2.txt-arity1
```

## Scenario 2

```
cr 1
fi data/disk1/ . 1
fi data/disk2/ . 1
start
take wiki-1.txt-arity1
take wiki-2.txt-arity1
take wiki-3.txt-arity1
take wiki-4.txt-arity1
```

## Scenario 3

Change `app.properties` field `slow_write` to `true` to debug this.

```
cr 1
cr 2
fi data/disk1/ . 1,2
start
take wiki-1.txt-arity1
poll wiki-2.txt-arity1
take wiki-1.txt-arity2
poll wiki-2.txt-arity2
merge wiki-1.txt-arity1,wiki-2.txt-arity1,wiki-1.txt-arity2,wiki-2.txt-arity2
```

## Scenario 4

Change `app.properties` field `slow_write` to `true` to debug this.

```
cr 1
cr 2
fi data/disk1/ . 1,2
start
take wiki-1.txt-arity1
poll wiki-2.txt-arity1
take wiki-1.txt-arity2
poll wiki-2.txt-arity2
merge wiki-1.txt-arity1,wiki-2.txt-arity1,wiki-1.txt-arity2,wiki-2.txt-arity2
```
