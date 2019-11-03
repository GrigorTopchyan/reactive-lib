package edu.test.reactive;

import edu.java.reactive.Flow;
import org.assertj.core.api.Assertions;
import org.reactivestreams.FlowAdapters;
import org.reactivestreams.Publisher;
import org.reactivestreams.tck.PublisherVerification;
import org.reactivestreams.tck.TestEnvironment;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.LongStream;


public class ArrayPublisherTest extends PublisherVerification<Long> {

    public ArrayPublisherTest() {
        super(new TestEnvironment());
    }

    @Test
    public void everyMethodInSubscribeShouldBeExecutedInParticularOrder() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        ArrayList<String> observedSignals = new ArrayList<>();
        Flow<Long> arrayPublisher = Flow.fromArray(generate(5));
        arrayPublisher.subscribe(new Subscriber<>() {
            @Override
            public void onSubscribe(Subscription subscription) {
                observedSignals.add("onSubscribe()");
                subscription.request(1000);
            }

            @Override
            public void onNext(Long el) {
                observedSignals.add("onNext(" + el + ")");
            }

            @Override
            public void onError(Throwable throwable) {

            }

            @Override
            public void onComplete() {
                observedSignals.add("onComplete()");
                latch.countDown();
            }
        });

        Assertions.assertThat(latch.await(1000, TimeUnit.MILLISECONDS)).isTrue();
        Assertions.assertThat(observedSignals).containsExactly(
                "onSubscribe()",
                "onNext(0)",
                "onNext(1)",
                "onNext(2)",
                "onNext(3)",
                "onNext(4)",
                "onComplete()"
        );
    }

    @Test
    public void mustSupportBackpresureControl() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        Long[] array = generate(10);
        Flow<Long> longArrayPublisher = Flow.fromArray(array);
        ArrayList<Long> collected = new ArrayList<>();
        Subscription[] subscriptions = new Subscription[1];

        longArrayPublisher.subscribe(new Subscriber<Long>() {
            @Override
            public void onSubscribe(Subscription subscription) {
                subscriptions[0] = subscription;
            }

            @Override
            public void onNext(Long el) {
                collected.add(el);
            }

            @Override
            public void onError(Throwable throwable) {

            }

            @Override
            public void onComplete() {
                latch.countDown();
            }
        });

        Assertions.assertThat(collected).isEmpty();

        subscriptions[0].request(1);
        Assertions.assertThat(collected).containsExactly(0L);


        subscriptions[0].request(1);
        Assertions.assertThat(collected).containsExactly(0L, 1L);


        subscriptions[0].request(2);
        Assertions.assertThat(collected).containsExactly(0L, 1L, 2L, 3L);


        subscriptions[0].request(20);
        Assertions.assertThat(latch.await(1, TimeUnit.SECONDS)).isTrue();

        Assertions.assertThat(collected).containsExactly(array);

    }

    @Test
    public void whenStreamElementIsNullThenMustSendNPE() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        Long[] array = new Long[]{null};
        Flow<Long> arrayPublisher = Flow.fromArray(array);
        AtomicReference<Throwable> error = new AtomicReference<>();
        arrayPublisher.subscribe(new Subscriber<>() {
            @Override
            public void onSubscribe(Subscription subscription) {
                subscription.request(4);
            }

            @Override
            public void onNext(Long aLong) {

            }

            @Override
            public void onError(Throwable throwable) {
                error.set(throwable);
                latch.countDown();
            }

            @Override
            public void onComplete() {

            }
        });

        latch.await(1, TimeUnit.SECONDS);
        Assertions.assertThat(error.get()).isInstanceOf(NullPointerException.class);
    }

    @Test
    public void shouldNotDieWithStackOverflow() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        ArrayList<Long> collected = new ArrayList<>();
        long toRequest = 10000L;
        Long[] array = generate(toRequest);
        Flow<Long> publisher = Flow.fromArray(array);

        publisher.subscribe(new Subscriber<Long>() {
            Subscription subscription;

            @Override
            public void onSubscribe(Subscription subscription) {
                this.subscription = subscription;
                subscription.request(1);
            }

            @Override
            public void onNext(Long el) {
                collected.add(el);
                subscription.request(1);
            }

            @Override
            public void onError(Throwable throwable) {

            }

            @Override
            public void onComplete() {
                latch.countDown();
            }
        });

        Assertions.assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
        Assertions.assertThat(collected).containsExactly(array);
    }

    @Test
    public void shluldBePossibleToCancelSubscriobtion() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        ArrayList<Long> collected = new ArrayList<>();
        long toRequest = 1000L;
        Long[] array = generate(toRequest);
        Flow<Long> publisher = Flow.fromArray(array);

        publisher.subscribe(new Subscriber<Long>() {
            @Override
            public void onSubscribe(Subscription subscription) {
                subscription.cancel();
                subscription.request(toRequest);
            }

            @Override
            public void onNext(Long el) {
                collected.add(el);
            }

            @Override
            public void onError(Throwable throwable) {

            }

            @Override
            public void onComplete() {
                latch.countDown();
            }
        });

        Assertions.assertThat(latch.await(1, TimeUnit.SECONDS)).isFalse();
        Assertions.assertThat(collected).isEmpty();
    }

    @Test
    public void shouldWorkCorrectlyInCaseOfErrorInMap() {
        Flow<Long> publisher = Flow.fromArray(generate(10));
        Flow<String> mapPublisher = publisher.map(el -> {
            throw new RuntimeException("For Test");
        });

        mapPublisher.subscribe(new Subscriber<String>() {
            boolean done = false;

            @Override
            public void onSubscribe(Subscription subscription) {
                subscription.request(100);
            }

            @Override
            public void onNext(String s) {
                Assertions.fail("Should not be called ever.");
            }

            @Override
            public void onError(Throwable throwable) {
                Assertions.assertThat(throwable)
                        .isInstanceOf(RuntimeException.class)
                        .hasMessage("For Test");
                Assertions.assertThat(done).isFalse();
                done = true;
            }

            @Override
            public void onComplete() {

            }
        });
    }

    @Test
    public void multiThreadingTest() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        ArrayList<Long> collected = new ArrayList<>();
        final int n = 5000;
        Long[] array = generate(n);
        Flow<Long> publisher = Flow.fromArray(array);

        publisher.subscribe(new Subscriber<Long>() {
            private Subscription subscription;

            @Override
            public void onSubscribe(Subscription subscription) {
                this.subscription = subscription;
                for (int i = 0; i < n; i++) {
                    ForkJoinPool.commonPool().execute(() -> subscription.request(1));
                }
            }

            @Override
            public void onNext(Long aLong) {
                collected.add(aLong);
            }

            @Override
            public void onError(Throwable throwable) {

            }

            @Override
            public void onComplete() {
                latch.countDown();
            }
        });

        latch.await(1, TimeUnit.SECONDS);
        Assertions.assertThat(collected)
                .hasSize(array.length)
                .containsExactly(array);
    }

    private static Long[] generate(long num) {
        return LongStream.range(0, num < Integer.MAX_VALUE ? num : 1000_000)
                .boxed()
                .toArray(Long[]::new);
    }

    @Override
    public Publisher<Long> createPublisher(long elements) {
        Flow<Long> flowPublisher = Flow.fromArray(generate(elements))
                .map(Object::toString)
                .map(Long::parseLong);
        return FlowAdapters.toPublisher(flowPublisher);
    }

    @Override
    public Publisher<Long> createFailedPublisher() {
        return null;
    }
}
