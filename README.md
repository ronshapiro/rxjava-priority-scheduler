# PriorityScheduler - RxJava
[![Build Status](https://travis-ci.org/ronshapiro/rxjava-priority-scheduler.svg?branch=master)](https://travis-ci.org/ronshapiro/rxjava-priority-scheduler)

While thinking about the intersection of [RxJava](https://github.com/ReactiveX/RxJava) and [Android](https://github.com/ReactiveX/RxAndroid), I realized there was no default scheduler in the library that allowed for prioritizing actions before others, similar to how [Volley](http://developer.android.com/training/volley/index.html)'s [Request.Priority](https://github.com/mcxiaoke/android-volley/blob/bea90385b1b847553a86425347fc3f560db98581/src/com/android/volley/Request.java#L503). I decided to try and work something together and this is what I initially came up with. Some of the threading seems a bit strange and the [Worker](https://github.com/ReactiveX/RxJava/blob/7dbed13ccc68bba80816311fe7c27126fe6d6d8f/src/main/java/rx/Scheduler.java#L60) works (no pun intended) a bit differently than others, but it seems to do the trick. Gladly accepting comments/pull requests!

## Sample Usage

```java
final int PRIORITY_HIGH = 10;
final int PRIORITY_LOW = 1;

PriorityScheduler scheduler = new PriorityScheduler();
Observable.just(1, 2, 3, 4, 5)
        .subscribeOn(scheduler.priority(PRIORITY_LOW))
        .subscribe(System.out::println);

Observable.just(6, 7, 8, 9, 10)
        .subscribeOn(scheduler.priority(PRIORITY_HIGH))
        .subscribe(System.out::println);
```

### Priorities
Priorties are simply ints ordered in increasing order. An action with a priority higher than another will be scheduled before (note that actions with the same priority may run in any order). Priorities may be any valid integer; you may want to define:
```java
private static final int PRIORITY_WHENEVER = Integer.MIN_VALUE;
````
and/or
```java
private static final int PRIORITY_NEXT = Integer.MAX_VALUE;
```

## Download

Gradle:
```groovy
compile 'me.ronshapiro.rx.priority:priority:0.2'
```

Since this is still a nascent idea, if you're using this library, please let me know ([@rdshapiro](https://twitter.com/rdshapiro)) how it's going!