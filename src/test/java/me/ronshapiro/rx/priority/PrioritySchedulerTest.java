package me.ronshapiro.rx.priority;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;

import rx.Observable;
import rx.functions.Action1;
import rx.schedulers.Schedulers;

import static org.junit.Assert.fail;

public class PrioritySchedulerTest {

    @Test
    public void schedulesInOrderOfPriority() throws Exception {
        PriorityScheduler scheduler = new PriorityScheduler(Executors.newSingleThreadExecutor());

        final int count = 1000;
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

        final int parallelism = scheduler.priority(0).parallelism();
        List<Integer> subList = actual.subList(parallelism, actual.size());
        int last = Integer.MAX_VALUE;
        for (int i : subList) {
            if (last < i) {
                System.out.println("actual: " + actual);
                fail("actual was not monotonically decreasing after the first N items, where N " +
                        "is the scheduler's parallelism/number of the cores on the machine. " +
                        "failed at index " + actual.indexOf(i) + " (value = " + i + "). " +
                        "Full list: " + actual);
            }
            last = i;
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}