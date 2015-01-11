package me.ronshapiro.rx.priority;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;

import rx.Observable;
import rx.functions.Action1;
import rx.schedulers.Schedulers;

import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;

@RunWith(JUnit4.class)
public class PrioritySchedulerTest {

    @Test
    public void schedulesInOrderOfPriorityWhenSingleThreaded() throws Exception {
        final int parallelism = 1;
        PriorityScheduler scheduler = PriorityScheduler.withConcurrency(parallelism);

        final int count = 10000;
        final CountDownLatch finishLatch = new CountDownLatch(count);
        final CountDownLatch loopLatch = new CountDownLatch(1);
        final List<Integer> actual = Collections.synchronizedList(new ArrayList<Integer>());
        final Object onNextLock = new Object();

        for (int i = 0; i < count; i++) {
            Observable.just(i)
                    .subscribeOn(scheduler.priority(i))
                    .subscribe(new Action1<Integer>() {
                        @Override
                        public void call(Integer integer) {
                            synchronized (onNextLock) {
                                await(loopLatch);
                                actual.add(integer);
                                finishLatch.countDown();
                            }
                        }
                    });
        }
        loopLatch.countDown();
        finishLatch.await();

        List<Integer> subList = actual.subList(parallelism, actual.size());
        int last = Integer.MAX_VALUE;
        for (int i : subList) {
            if (last < i) {
                fail("actual was not monotonically decreasing after the first N items, where N " +
                     "is the scheduler's parallelism. failed at index " + actual.indexOf(i) +
                     " (value = " + i + "). " + "Full list: " + actual);
            }
            last = i;
        }
    }

    @Test
    public void runsEachActionExactlyOnce_whenRunWithConcurrency() throws Exception {
        final int parallelism = 10;
        PriorityScheduler scheduler = PriorityScheduler.withConcurrency(parallelism);

        final int count = 10000;
        final CountDownLatch finishLatch = new CountDownLatch(count);
        final CountDownLatch loopLatch = new CountDownLatch(1);
        final Set<Integer> actual = Collections.synchronizedSet(new HashSet<Integer>());
        final Object onNextLock = new Object();

        for (int i = 0; i < count; i++) {
            Observable.just(i)
                    .subscribeOn(scheduler.priority(i))
                    .subscribe(new Action1<Integer>() {
                        @Override
                        public void call(Integer integer) {
                            synchronized (onNextLock) {
                                await(loopLatch);
                                actual.add(integer);
                                finishLatch.countDown();
                            }
                        }
                    });
        }
        loopLatch.countDown();
        finishLatch.await();

        assertEquals(count, actual.size());
    }


    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
