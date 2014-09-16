package me.ronshapiro.rx.priority;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;

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

    private final PriorityBlockingQueue<ComparableAction> queue;
    private volatile ExecutorService executorService;

    public PriorityScheduler() {
        this(null);
    }

    public PriorityScheduler(ExecutorService executorService) {
        this.queue = new PriorityBlockingQueue<ComparableAction>();
        this.executorService = executorService;
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
            return new PriorityWorker(queue, getExecutorService(), priority);
        }

        /** Lazy load the {@link ExecutorService}. See Effective Java: Item 71 */
        private ExecutorService getExecutorService() {
            ExecutorService result = executorService;
            if (result == null) {
                synchronized (PriorityScheduler.this) {
                    result = executorService;
                    if (result == null) {
                        executorService = result = Executors.newFixedThreadPool(parallelism());
                    }
                }
            }
            return result;
        }
    }

    private static final class PriorityWorker extends Worker {

        private final CompositeSubscription compositeSubscription = new CompositeSubscription();
        private final PriorityBlockingQueue<ComparableAction> queue;
        private final ExecutorService executorService;
        private final int priority;

        private PriorityWorker(PriorityBlockingQueue<ComparableAction> queue,
                ExecutorService executorService, int priority) {
            this.queue = queue;
            this.executorService = executorService;
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
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        queue.take().call();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            });
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
