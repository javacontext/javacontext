/*
 * Copyright 2020, JavaContext Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dev.javacontext.context;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.util.concurrent.SettableFuture;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link Context}. */
@RunWith(JUnit4.class)
@SuppressWarnings("CheckReturnValue") // false-positive in test for current ver errorprone plugin
public class ContextTest {

  private static final Context.Key<String> PET = Context.key("pet");
  private static final Context.Key<String> FOOD = Context.keyWithDefault("food", "lasagna");
  private static final Context.Key<String> COLOR = Context.key("color");
  private static final Context.Key<Object> FAVORITE = Context.key("favorite");
  private static final Context.Key<Integer> LUCKY = Context.key("lucky");

  private Context observed;
  private final Runnable runner =
      new Runnable() {
        @Override
        public void run() {
          observed = Context.current();
        }
      };
  private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

  @Before
  public void setUp() {
    Context.ROOT.attach();
  }

  @After
  public void tearDown() {
    scheduler.shutdown();
    assertEquals(Context.ROOT, Context.current());
  }

  @Test
  public void defaultContext() throws Exception {
    final SettableFuture<Context> contextOfNewThread = SettableFuture.create();
    Context contextOfThisThread = Context.ROOT.withValue(PET, "dog");
    Context toRestore = contextOfThisThread.attach();
    new Thread(
            new Runnable() {
              @Override
              public void run() {
                contextOfNewThread.set(Context.current());
              }
            })
        .start();
    assertNotNull(contextOfNewThread.get(5, TimeUnit.SECONDS));
    assertNotSame(contextOfThisThread, contextOfNewThread.get());
    assertSame(contextOfThisThread, Context.current());
    contextOfThisThread.detach(toRestore);
  }

  @Test
  public void rootCanBeAttached() {
    Context toRestore2 = Context.ROOT.attach();
    assertTrue(Context.ROOT.isCurrent());
    Context.ROOT.detach(toRestore2);
  }

  @Test
  public void attachingNonCurrentReturnsCurrent() {
    Context initial = Context.current();
    Context base = initial.withValue(PET, "dog");
    assertSame(initial, base.attach());
    assertSame(base, initial.attach());
  }

  @Test
  public void detachingNonCurrentLogsSevereMessage() {
    final AtomicReference<LogRecord> logRef = new AtomicReference<>();
    Handler handler =
        new Handler() {
          @Override
          public void publish(LogRecord record) {
            logRef.set(record);
          }

          @Override
          public void flush() {}

          @Override
          public void close() {}
        };
    Logger logger = Logger.getLogger(Context.storage().getClass().getName());
    try {
      logger.addHandler(handler);
      Context initial = Context.current();
      Context base = initial.withValue(PET, "dog");
      // Base is not attached
      base.detach(initial);
      assertSame(initial, Context.current());
      assertNotNull(logRef.get());
      assertEquals(Level.SEVERE, logRef.get().getLevel());
    } finally {
      logger.removeHandler(handler);
    }
  }

  @Test
  public void valuesAndOverrides() {
    Context base = Context.current().withValue(PET, "dog");
    Context child = base.withValues(PET, null, FOOD, "cheese");

    base.attach();

    assertEquals("dog", PET.get());
    assertEquals("lasagna", FOOD.get());
    assertNull(COLOR.get());

    child.attach();

    assertNull(PET.get());
    assertEquals("cheese", FOOD.get());
    assertNull(COLOR.get());

    child.detach(base);

    // Should have values from base
    assertEquals("dog", PET.get());
    assertEquals("lasagna", FOOD.get());
    assertNull(COLOR.get());

    base.detach(Context.ROOT);

    assertNull(PET.get());
    assertEquals("lasagna", FOOD.get());
    assertNull(COLOR.get());
  }

  @Test
  public void withValuesThree() {
    Object fav = new Object();
    Context base = Context.current().withValues(PET, "dog", COLOR, "blue");
    Context child = base.withValues(PET, "cat", FOOD, "cheese", FAVORITE, fav);

    Context toRestore = child.attach();

    assertEquals("cat", PET.get());
    assertEquals("cheese", FOOD.get());
    assertEquals("blue", COLOR.get());
    assertEquals(fav, FAVORITE.get());

    child.detach(toRestore);
  }

  @Test
  public void withValuesFour() {
    Object fav = new Object();
    Context base = Context.current().withValues(PET, "dog", COLOR, "blue");
    Context child = base.withValues(PET, "cat", FOOD, "cheese", FAVORITE, fav, LUCKY, 7);

    Context toRestore = child.attach();

    assertEquals("cat", PET.get());
    assertEquals("cheese", FOOD.get());
    assertEquals("blue", COLOR.get());
    assertEquals(fav, FAVORITE.get());
    assertEquals(7, (int) LUCKY.get());

    child.detach(toRestore);
  }

  @Test
  @SuppressWarnings("TryFailRefactoring")
  public void testWrapRunnable() {
    Context base = Context.current().withValue(PET, "cat");
    Context current = Context.current().withValue(PET, "fish");
    current.attach();

    base.wrap(runner).run();
    assertSame(base, observed);
    assertSame(current, Context.current());

    current.wrap(runner).run();
    assertSame(current, observed);
    assertSame(current, Context.current());

    base.run(runner);
    assertSame(base, observed);
    assertSame(current, Context.current());

    current.run(runner);
    assertSame(current, observed);
    assertSame(current, Context.current());

    final TestError err = new TestError();
    try {
      base.wrap(
              new Runnable() {
                @Override
                public void run() {
                  throw err;
                }
              })
          .run();
      fail("Expected exception");
    } catch (TestError ex) {
      assertSame(err, ex);
    }
    assertSame(current, Context.current());

    current.detach(Context.ROOT);
  }

  @Test
  @SuppressWarnings("TryFailRefactoring")
  public void testWrapCallable() throws Exception {
    Context base = Context.current().withValue(PET, "cat");
    Context current = Context.current().withValue(PET, "fish");
    current.attach();

    final Object ret = new Object();
    Callable<Object> callable =
        new Callable<Object>() {
          @Override
          public Object call() {
            runner.run();
            return ret;
          }
        };

    assertSame(ret, base.wrap(callable).call());
    assertSame(base, observed);
    assertSame(current, Context.current());

    assertSame(ret, current.wrap(callable).call());
    assertSame(current, observed);
    assertSame(current, Context.current());

    assertSame(ret, base.call(callable));
    assertSame(base, observed);
    assertSame(current, Context.current());

    assertSame(ret, current.call(callable));
    assertSame(current, observed);
    assertSame(current, Context.current());

    final TestError err = new TestError();
    try {
      base.wrap(
              new Callable<Object>() {
                @Override
                public Object call() {
                  throw err;
                }
              })
          .call();
      fail("Excepted exception");
    } catch (TestError ex) {
      assertSame(err, ex);
    }
    assertSame(current, Context.current());

    current.detach(Context.ROOT);
  }

  @Test
  public void currentContextExecutor() {
    QueuedExecutor queuedExecutor = new QueuedExecutor();
    Executor executor = Context.currentContextExecutor(queuedExecutor);
    Context base = Context.current().withValue(PET, "cat");
    Context previous = base.attach();
    try {
      executor.execute(runner);
    } finally {
      base.detach(previous);
    }
    assertEquals(1, queuedExecutor.runnables.size());
    queuedExecutor.runnables.remove().run();
    assertSame(base, observed);
  }

  @Test
  public void fixedContextExecutor() {
    Context base = Context.current().withValue(PET, "cat");
    QueuedExecutor queuedExecutor = new QueuedExecutor();
    base.fixedContextExecutor(queuedExecutor).execute(runner);
    assertEquals(1, queuedExecutor.runnables.size());
    queuedExecutor.runnables.remove().run();
    assertSame(base, observed);
  }

  @Test
  public void typicalTryFinallyHandling() {
    Context base = Context.current().withValue(COLOR, "blue");
    Context previous = base.attach();
    try {
      assertTrue(base.isCurrent());
      // Do something
    } finally {
      base.detach(previous);
    }
    assertFalse(base.isCurrent());
  }

  private static class QueuedExecutor implements Executor {
    private final Queue<Runnable> runnables = new ArrayDeque<>();

    @Override
    public void execute(Runnable r) {
      runnables.add(r);
    }
  }

  /**
   * Tests initializing the {@link Context} class with a custom logger which uses Context's storage
   * when logging.
   */
  @Test
  public void initContextWithCustomClassLoaderWithCustomLogger() throws Exception {
    StaticTestingClassLoader classLoader =
        new StaticTestingClassLoader(
            getClass().getClassLoader(), Pattern.compile("io\\.grpc\\.[^.]+"));
    Class<?> runnable = classLoader.loadClass(LoadMeWithStaticTestingClassLoader.class.getName());

    ((Runnable) runnable.getDeclaredConstructor().newInstance()).run();
  }

  /**
   * Ensure that newly created threads can attach/detach a context. The current test thread already
   * has a context manually attached in {@link #setUp()}.
   */
  @Test
  public void newThreadAttachContext() throws Exception {
    Context parent = Context.current().withValue(COLOR, "blue");
    parent.call(
        new Callable<Object>() {
          @Override
          @Nullable
          public Object call() throws Exception {
            assertEquals("blue", COLOR.get());

            final Context child = Context.current().withValue(COLOR, "red");
            Future<String> workerThreadVal =
                scheduler.submit(
                    new Callable<String>() {
                      @Override
                      public String call() {
                        Context initial = Context.current();
                        assertNotNull(initial);
                        Context toRestore = child.attach();
                        try {
                          assertNotNull(toRestore);
                          return COLOR.get();
                        } finally {
                          child.detach(toRestore);
                          assertEquals(initial, Context.current());
                        }
                      }
                    });
            assertEquals("red", workerThreadVal.get());

            assertEquals("blue", COLOR.get());
            return null;
          }
        });
  }

  /**
   * Similar to {@link #newThreadAttachContext()} but without giving the new thread a specific ctx.
   */
  @Test
  public void newThreadWithoutContext() throws Exception {
    Context parent = Context.current().withValue(COLOR, "blue");
    parent.call(
        new Callable<Object>() {
          @Override
          @Nullable
          public Object call() throws Exception {
            assertEquals("blue", COLOR.get());

            Future<String> workerThreadVal =
                scheduler.submit(
                    new Callable<String>() {
                      @Override
                      public String call() {
                        assertNotNull(Context.current());
                        return COLOR.get();
                      }
                    });
            assertNull(workerThreadVal.get());

            assertEquals("blue", COLOR.get());
            return null;
          }
        });
  }

  @Test
  public void storageReturnsNullTest() throws Exception {
    Class<?> lazyStorageClass = Class.forName("dev.javacontext.context.Context$LazyStorage");
    Field storage = lazyStorageClass.getDeclaredField("storage");
    assertTrue(Modifier.isFinal(storage.getModifiers()));
    // use reflection to forcibly change the storage object to a test object
    storage.setAccessible(true);
    Field modifiersField = Field.class.getDeclaredField("modifiers");
    modifiersField.setAccessible(true);
    int storageModifiers = modifiersField.getInt(storage);
    modifiersField.set(storage, storageModifiers & ~Modifier.FINAL);
    Object o = storage.get(null);
    Context.Storage originalStorage = (Context.Storage) o;
    try {
      storage.set(
          null,
          new Context.Storage() {
            @Override
            @Nullable
            public Context doAttach(Context toAttach) {
              return null;
            }

            @Override
            public void detach(Context toDetach, Context toRestore) {
              // noop
            }

            @Override
            @Nullable
            public Context current() {
              return null;
            }
          });
      // current() returning null gets transformed into ROOT
      assertEquals(Context.ROOT, Context.current());

      // doAttach() returning null gets transformed into ROOT
      Context blueContext = Context.current().withValue(COLOR, "blue");
      Context toRestore = blueContext.attach();
      assertEquals(Context.ROOT, toRestore);

      // final sanity check
      blueContext.detach(toRestore);
      assertEquals(Context.ROOT, Context.current());
    } finally {
      // undo the changes
      storage.set(null, originalStorage);
      storage.setAccessible(false);
      modifiersField.set(storage, storageModifiers | Modifier.FINAL);
      modifiersField.setAccessible(false);
    }
  }

  @Test
  public void errorWhenAncestryLengthLong() {
    final AtomicReference<LogRecord> logRef = new AtomicReference<>();
    Handler handler =
        new Handler() {
          @Override
          public void publish(LogRecord record) {
            logRef.set(record);
          }

          @Override
          public void flush() {}

          @Override
          public void close() {}
        };
    Logger logger = Logger.getLogger(Context.class.getName());
    try {
      logger.addHandler(handler);
      Context ctx = Context.current();
      for (int i = 0; i < Context.CONTEXT_DEPTH_WARN_THRESH; i++) {
        assertNull(logRef.get());
        ctx = ctx.withValue(PET, "tiger");
      }
      ctx = ctx.withValue(PET, "lion");
      assertEquals("lion", PET.get(ctx));
      assertNotNull(logRef.get());
      assertNotNull(logRef.get().getThrown());
      assertEquals(Level.SEVERE, logRef.get().getLevel());
    } finally {
      logger.removeHandler(handler);
    }
  }

  // UsedReflectively
  public static final class LoadMeWithStaticTestingClassLoader implements Runnable {
    @Override
    public void run() {
      Logger logger = Logger.getLogger(Context.class.getName());
      logger.setLevel(Level.ALL);
      Handler handler =
          new Handler() {
            @Override
            public void publish(LogRecord record) {
              Context ctx = Context.current();
              Context previous = ctx.attach();
              ctx.detach(previous);
            }

            @Override
            public void flush() {}

            @Override
            public void close() {}
          };
      logger.addHandler(handler);

      try {
        assertNotNull(Context.ROOT);
      } finally {
        logger.removeHandler(handler);
      }
    }
  }

  /** Allows more precise catch blocks than plain Error to avoid catching AssertionError. */
  private static final class TestError extends Error {}
}
