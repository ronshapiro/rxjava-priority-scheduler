package me.ronshapiro.rx.priority;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import io.reactivex.Scheduler;
import io.reactivex.annotations.NonNull;
import io.reactivex.disposables.CompositeDisposable;
import io.reactivex.disposables.Disposable;
import io.reactivex.internal.schedulers.ScheduledRunnable;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * A class to be used with RxJava's {@link Scheduler} interface. Though this class is not a {@link
 * Scheduler} itself, calling {@link #priority(int)} will return one. E.x.: {@code PriorityScheduler
 * scheduler = new PriorityScheduler(); Observable.just(1, 2, 3) .subscribeOn(scheduler.priority(10))
 * .subscribe(); }
 */
public final class PriorityScheduler {

    private final PriorityBlockingQueue<ComparableRunnable> queue = new PriorityBlockingQueue<>();
    private final AtomicInteger workerCount = new AtomicInteger();
    private final int concurrency;
    private ExecutorService executorService;

    /**
     * Creates a {@link PriorityScheduler} with as many threads as the machine's available
     * processors.
     * <p>
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
     * <p>
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

    public static PriorityScheduler get() {
        return Holder.INSTANCE;
    }

    private static class Holder {
        static PriorityScheduler INSTANCE = create();
    }

    /**
     * Prioritize {@link io.reactivex.functions.Action  action}s with a numerical priority
     * value. The higher the priority, the sooner it will run.
     */
    public Scheduler priority(final int priority) {
        return new InnerPriorityScheduler(priority);
    }

    private final class InnerPriorityScheduler extends Scheduler {

        private final int priority;

        private InnerPriorityScheduler(int priority) {
            this.priority = priority;
        }

        @Override public Worker createWorker() {
            synchronized (workerCount) {
                if (workerCount.get() < concurrency) {
                    workerCount.incrementAndGet();
                    executorService.submit(new Runnable() {
                        @Override public void run() {
                            while (true) {
                                try {
                                    ComparableRunnable runnable = queue.take();
                                    runnable.run();
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

    private static final class PriorityWorker extends Scheduler.Worker {

        private final CompositeDisposable compositeDisposable = new CompositeDisposable();
        private final PriorityBlockingQueue<ComparableRunnable> queue;
        private final int priority;

        private PriorityWorker(PriorityBlockingQueue<ComparableRunnable> queue, int priority) {
            this.queue = queue;
            this.priority = priority;
        }

        @Override public Disposable schedule(Runnable action) {
            return schedule(action, 0, MILLISECONDS);
        }

        /**
         * inspired by HandlerThreadScheduler.InnerHandlerThreadScheduler#schedule.
         * @see <a href="https://github.com/ReactiveX/RxAndroid/blob/53bc70785b1c8f150c2be871a5b85979ad8b233a/src/main/java/rx/android/schedulers/HandlerThreadScheduler.java">InnerHandlerThreadScheduler</a>
         */
        @Override public Disposable schedule(@NonNull Runnable run, long delayTime, @NonNull TimeUnit unit) {
            final ComparableRunnable runnable = new ComparableRunnable(run, priority);

            final ScheduledRunnable scheduledRunnable = new ScheduledRunnable(runnable, compositeDisposable);
            scheduledRunnable.setFuture(new Future<Object>() {
                @Override public boolean cancel(boolean b) {
                    return queue.remove(runnable);
                }

                @Override public boolean isCancelled() {
                    return false;
                }

                @Override public boolean isDone() {
                    return false;
                }

                @Override public Object get() throws InterruptedException, ExecutionException {
                    return null;
                }

                @Override public Object get(long l, @NonNull TimeUnit timeUnit)
                        throws InterruptedException, ExecutionException, TimeoutException {
                    return null;
                }
            });
            compositeDisposable.add(scheduledRunnable);

            queue.offer(runnable, delayTime, unit);
            return scheduledRunnable;
        }

        @Override public void dispose() {
            compositeDisposable.dispose();
        }

        @Override public boolean isDisposed() {
            return compositeDisposable.isDisposed();
        }

    }

    private static final class ComparableRunnable implements Runnable, Comparable<ComparableRunnable> {

        private final Runnable runnable;
        private final int priority;

        private ComparableRunnable(Runnable runnable, int priority) {
            this.runnable = runnable;
            this.priority = priority;
        }

        @Override public void run() {
            runnable.run();
        }

        @Override public int compareTo(ComparableRunnable o) {
            return o.priority - priority;
        }
    }
}
