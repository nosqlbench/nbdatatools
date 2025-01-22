This has some ideas on how to present vector neighborhoods on console

## progress
```
3/30  25% [==========>                  ] 5000/1000000 (time estimate)
```

## bitfields
```
⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿
```

## detailed neighbor set ordinals
```
all  23 46 234 304 345 2348 3423 83840 ...
v1   23 46 234     345 2348 3423       ...
v2   23    234 304 345      3423 83840 ...
```

### as a bitfield
```
v1 ⣿⠿⣿...
v2 ⣿⣭⣿...
    ^ highlighted when not full set
```

## scrolling status
```
[0] FAIL: ⣿⠿⣿⣿⣭⣿
23 46 234     345 2348 3423
23    234 304 345      3423 83840
[1] PASS
 \+ (unroll only if you really want to see the details)
[2] PASS
```

## scribbles

```
X X X _ X X X _
X _ X X X _ X X

⣿⠿⣿
⣿⣭⣿

⣿⣿⣿⣿⣿⣿⡸⣿⣿⣿
```

