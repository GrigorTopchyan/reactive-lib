package edu.java.reactive;

import java.util.Objects;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

public abstract class Flow<T> implements Publisher<T> {
    @SafeVarargs
    public static <E> Flow<E> fromArray(E... elements) {
        return new ArrayPublisher<>(elements);
    }

    public <K> Flow<K> map(Function<T, K> mapper) {
        return new MapPublisher<>(this, mapper);
    }

    private static class ArrayPublisher<T> extends Flow<T> {
        private final T[] sequence;

        private ArrayPublisher(T[] sequence) {
            this.sequence = sequence;
        }

        @Override
        public void subscribe(java.util.concurrent.Flow.Subscriber<? super T> subscriber) {
            subscriber.onSubscribe(new java.util.concurrent.Flow.Subscription() {
                int current;
                AtomicLong requested = new AtomicLong();
                volatile boolean isCanceled = false;

                @Override
                public void request(long l) {
                    if (l <= 0 && !isCanceled) {
                        cancel();
                        subscriber.onError(new IllegalArgumentException("Contract violates. Positive request amount required but it was " + l));
                    }
                    long initialRequested;

                    do {
                        initialRequested = requested.get();
                        if (initialRequested == Long.MAX_VALUE) {
                            return;
                        }
                        l = initialRequested + l;
                        if (l < 0) {
                            l = Long.MAX_VALUE;
                        }
                    } while (!requested.compareAndSet(initialRequested, l));

                    if (initialRequested > 0) {
                        return;
                    }
                    for (; requested.get() > 0 && current < sequence.length && !isCanceled; ) {
                        T el = sequence[current++];
                        if (el == null) {
                            subscriber.onError(new NullPointerException());
                            cancel();
                            return;
                        }
                        subscriber.onNext(el);
                        long req = requested.decrementAndGet();
                        if (req == 0) {
                            return;
                        }
                    }
                    if (current == sequence.length) {
                        subscriber.onComplete();
                    }
                }

                @Override
                public void cancel() {
                    isCanceled = true;
                }
            });
        }
    }

    private static class MapPublisher<IN, OUT> extends Flow<OUT> {
        private final Publisher<IN> parent;
        private final Function<IN, OUT> mapper;

        private MapPublisher(Publisher<IN> parent, Function<IN, OUT> mapper) {
            this.parent = parent;
            this.mapper = mapper;
        }

        @Override
        public void subscribe(java.util.concurrent.Flow.Subscriber<? super OUT> actual) {
            parent.subscribe(new MapSubscriber<>(mapper, actual));
        }

        public static class MapSubscriber<IN, OUT> implements java.util.concurrent.Flow.Subscriber<IN>, java.util.concurrent.Flow.Subscription {
            final Function<IN, OUT> mapper;
            final java.util.concurrent.Flow.Subscriber<? super OUT> actual;
            private java.util.concurrent.Flow.Subscription upStream;
            private boolean terminated;

            MapSubscriber(Function<IN, OUT> mapper, java.util.concurrent.Flow.Subscriber<? super OUT> actual) {
                this.mapper = mapper;
                this.actual = actual;
            }

            @Override
            public void onSubscribe(java.util.concurrent.Flow.Subscription subscription) {
                this.upStream = subscription;
                actual.onSubscribe(this);
            }

            @Override
            public void onNext(IN o) {
                if (terminated) {
                    return;
                }
                OUT resultValue = null;
                try {
                    resultValue = Objects.requireNonNull(mapper.apply(o));
                } catch (Throwable t) {
                    cancel();
                    onError(t);
                    return;
                }
                actual.onNext(resultValue);
            }

            @Override
            public void onError(Throwable throwable) {
                if (terminated) {
                    return;
                }
                actual.onError(throwable);
                terminated = true;
            }

            @Override
            public void onComplete() {
                if (terminated) {
                    return;
                }
                terminated = true;
                actual.onComplete();
            }

            @Override
            public void request(long l) {
                upStream.request(l);
            }

            @Override
            public void cancel() {
                upStream.cancel();
            }
        }
    }
}
