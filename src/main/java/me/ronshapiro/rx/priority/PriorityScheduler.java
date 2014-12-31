package me.ronshapiro.rx.priority;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import rx.Scheduler;
import rx.Scheduler.Worker;
import rx.Subscription;
import rx.functions.Action0;
import rx.internal.schedulers.ScheduledAction;
import rx.subscriptions.CompositeSubscription;
import rx.subscriptions.Subscriptions;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * A class to be used with RxJava's {@link Scheduler} interface. Though this class is not a {@link
 * Scheduler} itself, calling {@link #priority(int)} will return one. E.x.: {@code PriorityScheduler
 * scheduler = new PriorityScheduler(); Observable.just(1, 2, 3) .subscribeOn(scheduler.priority(10))
 * .subscribe(); }
 */
public final class PriorityScheduler {

    private final PriorityBlockingQueue<ComparableAction> queue =
        new PriorityBlockingQueue<ComparableAction>();
    private final AtomicInteger workerCount = new AtomicInteger();
    private final int concurrency;
    private ExecutorService executorService;

    /**
     * Creates a {@link PriorityScheduler} with as many threads as the machine's available
     * processors.
     *
     * <b>Note:</b> this does not ensure that the priorities will be adheared to exactly, as the
     * JVM's threading policy might allow one thread to dequeue an action, then let a second thread
     * dequeue the next action, run it, dequeue another, run it, etc. before the first thread runs
     * its action. It does however ensure that at the dequeue step, the thread will receive the
     * highest priority action available.
     */
    public static PriorityScheduler create() {
        return new PriorityScheduler(Runtime.getRuntime().availableProcessors());
    }

    /**
     * Creates a {@link PriorityScheduler} using at most {@code concurrency} concurrent actions.
     *
     * <b>Note:</b> this does not ensure that the priorities will be adheared to exactly, as the
     * JVM's threading policy might allow one thread to dequeue an action, then let a second thread
     * dequeue the next action, run it, dequeue another, run it, etc. before the first thread runs
     * its action. It does however ensure that at the dequeue step, the thread will receive the
     * highest priority action available.
     */
    public static PriorityScheduler withConcurrency(int concurrency) {
        return new PriorityScheduler(concurrency);
    }

    private PriorityScheduler(int concurrency) {
        this.executorService = Executors.newFixedThreadPool(concurrency);
        this.concurrency = concurrency;
    }

    /**
     * Prioritize {@link rx.functions.Action action}s with a numerical priority value. The higher
     * the priority, the sooner it will run.
     */
    public Scheduler priority(final int priority) {
        return new InnerPriorityScheduler(priority);
    }

    private final class InnerPriorityScheduler extends Scheduler {

        private final int priority;

        private InnerPriorityScheduler(int priority) {
            this.priority = priority;
        }

        @Override
        public Worker createWorker() {
            synchronized (workerCount) {
                if (workerCount.get() < concurrency) {
                    workerCount.incrementAndGet();
                    executorService.submit(new Runnable() {
                        @Override
                        public void run() {
                            while (true) {
                                try {
                                    ComparableAction action = queue.take();
                                    action.call();
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                    break;
                                }
                            }
                        }
                    });
                }
            }
            return new PriorityWorker(queue, priority);
        }
    }

    private static final class PriorityWorker extends Worker {

        private final CompositeSubscription compositeSubscription = new CompositeSubscription();
        private final PriorityBlockingQueue<ComparableAction> queue;
        private final int priority;

        private PriorityWorker(PriorityBlockingQueue<ComparableAction> queue, int priority) {
            this.queue = queue;
            this.priority = priority;
        }

        @Override
        public Subscription schedule(Action0 action) {
            return schedule(action, 0, MILLISECONDS);
        }

        /**
         * inspired by HandlerThreadScheduler.InnerHandlerThreadScheduler#schedule.
         *
         * @see <a href="https://github.com/ReactiveX/RxAndroid/blob/53bc70785b1c8f150c2be871a5b85979ad8b233a/src/main/java/rx/android/schedulers/HandlerThreadScheduler.java">InnerHandlerThreadScheduler</a>
         */
        @Override
        public Subscription schedule(Action0 action, long delayTime, TimeUnit unit) {
            final ComparableAction comparableAction = new ComparableAction(action, priority);

            final ScheduledAction scheduledAction = new ScheduledAction(comparableAction);
            scheduledAction.add(Subscriptions.create(new Action0() {
                @Override
                public void call() {
                    queue.remove(comparableAction);
                }
            }));
            scheduledAction.addParent(compositeSubscription);
            compositeSubscription.add(scheduledAction);

            queue.offer(comparableAction, delayTime, unit);
            return scheduledAction;
        }

        @Override
        public void unsubscribe() {
            compositeSubscription.unsubscribe();
        }

        @Override
        public boolean isUnsubscribed() {
            return compositeSubscription.isUnsubscribed();
        }

    }

    private static final class ComparableAction implements Action0, Comparable<ComparableAction> {

        private final Action0 action;
        private final int priority;

        private ComparableAction(Action0 action, int priority) {
            this.action = action;
            this.priority = priority;
        }

        @Override
        public void call() {
            action.call();
        }

        @Override
        public int compareTo(ComparableAction o) {
            return o.priority - priority;
        }
    }
}
