# KDS Domaci 1

## Scenario 1

```
cr 1
fi create data/disk1
fi cr add 1 1
fi dir add 1 A
start
take wiki-1.txt-arity1
take wiki-2.txt-arity1
merge wiki-1.txt-arity1,wiki-2.txt-arity1
take m-wiki-1.txt-arity1-wiki-2.txt-arity1
```

## Scenario 2

```
cr 1
fi create data/disk1
fi create data/disk2
fi cr add 1 1
fi cr add 2 1
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
fi create data/disk1
fi cr add 1 1
fi cr add 1 2
start
take wiki-1.txt-arity1
poll wiki-2.txt-arity1
take wiki-1.txt-arity2
poll wiki-2.txt-arity2
merge wiki-1.txt-arity1,wiki-2.txt-arity1,wiki-1.txt-arity2,wiki-2.txt-arity2
```

## Scenario 4

```
cr 1
cr 2
fi create data/disk1
fi cr add 1 1
start
# wait until end
fi cr add 1 2
# change file or add a new one
```
