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

import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/**
 * A context propagation mechanism which can carry scoped-values across API boundaries and between
 * threads. Examples of state propagated via context include:
 *
 * <ul>
 *   <li>Security principals and credentials.
 *   <li>Local and distributed tracing information.
 * </ul>
 *
 * <p>A Context object can be {@link #attach attached} to the {@link Storage}, which effectively
 * forms a <b>scope</b> for the context. The scope is bound to the current thread. Within a scope,
 * its Context is accessible even across API boundaries, through {@link #current}. The scope is
 * later exited by {@link #detach detaching} the Context.
 *
 * <p>Context objects are immutable and inherit state from their parent. To add or overwrite the
 * current state a new context object must be created and then attached, replacing the previously
 * bound context. For example:
 *
 * <pre>
 *   Context withCredential = Context.current().withValue(CRED_KEY, cred);
 *   withCredential.run(new Runnable() {
 *     public void run() {
 *        readUserRecords(userId, CRED_KEY.get());
 *     }
 *   });
 * </pre>
 *
 * <p>Notes and cautions on use:
 *
 * <ul>
 *   <li>Every {@code attach()} should have a {@code detach()} in the same method. Breaking this
 *       rules may lead to memory leaks.
 *   <li>While Context objects are immutable they do not place such a restriction on the state they
 *       store.
 *   <li>Context is not intended for passing optional parameters to an API and developers should
 *       take care to avoid excessive dependence on context when designing an API.
 *   <li>Do not mock this class. Use {@link #ROOT} for a non-null instance.
 * </ul>
 */
/* @DoNotMock("Use ROOT for a non-null Context") // commented out to avoid dependencies  */
public class Context {
  private static final Logger log = Logger.getLogger(Context.class.getName());

  private static final PersistentHashArrayMappedTrie<Key<?>, Object> EMPTY_ENTRIES =
      new PersistentHashArrayMappedTrie<>();

  // Long chains of contexts are suspicious and usually indicate a misuse of Context.
  // The threshold is arbitrarily chosen.
  // VisibleForTesting
  static final int CONTEXT_DEPTH_WARN_THRESH = 1000;

  /**
   * The logical root context which is the ultimate ancestor of all contexts.
   *
   * <p>Never assume this is the default context for new threads, because {@link Storage} may define
   * a default context that is different from ROOT.
   */
  public static final Context ROOT = new Context(null, EMPTY_ENTRIES);

  // VisibleForTesting
  static Storage storage() {
    return LazyStorage.storage;
  }

  // Lazy-loaded storage. Delaying storage initialization until after class initialization makes it
  // much easier to avoid circular loading since there can still be references to Context as long as
  // they don't depend on storage, like key() and currentContextExecutor(). It also makes it easier
  // to handle exceptions.
  private static final class LazyStorage {
    static final Storage storage;

    static {
      AtomicReference<Throwable> deferredStorageFailure = new AtomicReference<>();
      storage = createStorage(deferredStorageFailure);
      Throwable failure = deferredStorageFailure.get();
      // Logging must happen after storage has been set, as loggers may use Context.
      if (failure != null) {
        log.log(Level.FINE, "Storage override doesn't exist. Using default", failure);
      }
    }

    private static Storage createStorage(
        AtomicReference<? super ClassNotFoundException> deferredStorageFailure) {
      try {
        Class<?> clazz = Class.forName("io.grpc.override.ContextStorageOverride");
        // The override's constructor is prohibited from triggering any code that can loop back to
        // Context
        return clazz.asSubclass(Storage.class).getConstructor().newInstance();
      } catch (ClassNotFoundException e) {
        deferredStorageFailure.set(e);
        return new ThreadLocalContextStorage();
      } catch (Exception e) {
        throw new RuntimeException("Storage override failed to initialize", e);
      }
    }
  }

  /**
   * Create a {@link Key} with the given debug name. Multiple different keys may have the same name;
   * the name is intended for debugging purposes and does not impact behavior.
   */
  public static <T> Key<T> key(String name) {
    return new Key<>(name);
  }

  /**
   * Create a {@link Key} with the given debug name and default value. Multiple different keys may
   * have the same name; the name is intended for debugging purposes and does not impact behavior.
   */
  public static <T> Key<T> keyWithDefault(String name, T defaultValue) {
    return new Key<>(name, defaultValue);
  }

  /** Return the context associated with the current scope, will never return {@code null}. */
  public static Context current() {
    Context current = storage().current();
    if (current == null) {
      return ROOT;
    }
    return current;
  }

  final PersistentHashArrayMappedTrie<Key<?>, Object> keyValueEntries;
  // The number parents between this context and the root context.
  final int generation;

  /**
   * Construct a context that cannot be cancelled but will cascade cancellation from its parent if
   * it is cancellable.
   */
  private Context(Context parent, PersistentHashArrayMappedTrie<Key<?>, Object> keyValueEntries) {
    this.keyValueEntries = keyValueEntries;
    this.generation = parent == null ? 0 : parent.generation + 1;
    validateGeneration(generation);
  }

  /**
   * Create a new context with the given key value set.
   *
   * <pre>
   *   Context withCredential = Context.current().withValue(CRED_KEY, cred);
   *   withCredential.run(new Runnable() {
   *     public void run() {
   *        readUserRecords(userId, CRED_KEY.get());
   *     }
   *   });
   * </pre>
   *
   * <p>Note that multiple calls to {@link #withValue} can be chained together. That is,
   *
   * <pre>
   * context.withValues(K1, V1, K2, V2);
   * // is the same as
   * context.withValue(K1, V1).withValue(K2, V2);
   * </pre>
   *
   * <p>Nonetheless, {@link Context} should not be treated like a general purpose map with a large
   * number of keys and values — combine multiple related items together into a single key instead
   * of separating them. But if the items are unrelated, have separate keys for them.
   */
  public <V> Context withValue(Key<V> k1, V v1) {
    PersistentHashArrayMappedTrie<Key<?>, Object> newKeyValueEntries = keyValueEntries.put(k1, v1);
    return new Context(this, newKeyValueEntries);
  }

  /** Create a new context with the given key value set. */
  public <V1, V2> Context withValues(Key<V1> k1, V1 v1, Key<V2> k2, V2 v2) {
    PersistentHashArrayMappedTrie<Key<?>, Object> newKeyValueEntries =
        keyValueEntries.put(k1, v1).put(k2, v2);
    return new Context(this, newKeyValueEntries);
  }

  /** Create a new context with the given key value set. */
  public <V1, V2, V3> Context withValues(Key<V1> k1, V1 v1, Key<V2> k2, V2 v2, Key<V3> k3, V3 v3) {
    PersistentHashArrayMappedTrie<Key<?>, Object> newKeyValueEntries =
        keyValueEntries.put(k1, v1).put(k2, v2).put(k3, v3);
    return new Context(this, newKeyValueEntries);
  }

  /**
   * Create a new context with the given key value set.
   *
   * <p>For more than 4 key-value pairs, note that multiple calls to {@link #withValue} can be
   * chained together. That is,
   *
   * <pre>
   * context.withValues(K1, V1, K2, V2);
   * // is the same as
   * context.withValue(K1, V1).withValue(K2, V2);
   * </pre>
   *
   * <p>Nonetheless, {@link Context} should not be treated like a general purpose map with a large
   * number of keys and values — combine multiple related items together into a single key instead
   * of separating them. But if the items are unrelated, have separate keys for them.
   */
  public <V1, V2, V3, V4> Context withValues(
      Key<V1> k1, V1 v1, Key<V2> k2, V2 v2, Key<V3> k3, V3 v3, Key<V4> k4, V4 v4) {
    PersistentHashArrayMappedTrie<Key<?>, Object> newKeyValueEntries =
        keyValueEntries.put(k1, v1).put(k2, v2).put(k3, v3).put(k4, v4);
    return new Context(this, newKeyValueEntries);
  }

  /**
   * Attach this context, thus enter a new scope within which this context is {@link #current}. The
   * previously current context is returned.
   *
   * <p>Instead of using {@code attach()} and {@link #detach(Context)} most use-cases are better
   * served by using the {@link #run(Runnable)} or {@link #call(java.util.concurrent.Callable)} to
   * execute work immediately within a context's scope. If work needs to be done in other threads it
   * is recommended to use the 'wrap' methods or to use a propagating executor.
   *
   * <p>All calls to {@code attach()} should have a corresponding {@link #detach(Context)} within
   * the same method:
   *
   * <pre>{@code Context previous = someContext.attach();
   * try {
   *   // Do work
   * } finally {
   *   someContext.detach(previous);
   * }}</pre>
   */
  public Context attach() {
    Context prev = storage().doAttach(this);
    if (prev == null) {
      return ROOT;
    }
    return prev;
  }

  /**
   * Reverse an {@code attach()}, restoring the previous context and exiting the current scope.
   *
   * <p>This context should be the same context that was previously {@link #attach attached}. The
   * provided replacement should be what was returned by the same {@link #attach attach()} call. If
   * an {@code attach()} and a {@code detach()} meet above requirements, they match.
   *
   * <p>It is expected that between any pair of matching {@code attach()} and {@code detach()}, all
   * {@code attach()}es and {@code detach()}es are called in matching pairs. If this method finds
   * that this context is not {@link #current current}, either you or some code in-between are not
   * detaching correctly, and a SEVERE message will be logged but the context to attach will still
   * be bound. <strong>Never</strong> use {@code Context.current().detach()}, as this will
   * compromise this error-detecting mechanism.
   */
  public void detach(Context toAttach) {
    checkNotNull(toAttach, "toAttach");
    storage().detach(this, toAttach);
  }

  // Visible for testing
  boolean isCurrent() {
    return current() == this;
  }

  /**
   * Immediately run a {@link Runnable} with this context as the {@link #current} context.
   *
   * @param r {@link Runnable} to run.
   */
  public void run(Runnable r) {
    Context previous = attach();
    try {
      r.run();
    } finally {
      detach(previous);
    }
  }

  /**
   * Immediately call a {@link Callable} with this context as the {@link #current} context.
   *
   * @param c {@link Callable} to call.
   * @return result of call.
   */
  public <V> V call(Callable<V> c) throws Exception {
    Context previous = attach();
    try {
      return c.call();
    } finally {
      detach(previous);
    }
  }

  /**
   * Wrap a {@link Runnable} so that it executes with this context as the {@link #current} context.
   */
  public Runnable wrap(final Runnable r) {
    return new Runnable() {
      @Override
      public void run() {
        Context previous = attach();
        try {
          r.run();
        } finally {
          detach(previous);
        }
      }
    };
  }

  /**
   * Wrap a {@link Callable} so that it executes with this context as the {@link #current} context.
   */
  public <C> Callable<C> wrap(final Callable<C> c) {
    return new Callable<C>() {
      @Override
      public C call() throws Exception {
        Context previous = attach();
        try {
          return c.call();
        } finally {
          detach(previous);
        }
      }
    };
  }

  /**
   * Wrap an {@link Executor} so that it always executes with this context as the {@link #current}
   * context. It is generally expected that {@link #currentContextExecutor(Executor)} would be used
   * more commonly than this method.
   *
   * <p>One scenario in which this executor may be useful is when a single thread is sharding work
   * to multiple threads.
   *
   * @see #currentContextExecutor(Executor)
   */
  public Executor fixedContextExecutor(final Executor e) {
    final class FixedContextExecutor implements Executor {
      @Override
      public void execute(Runnable r) {
        e.execute(wrap(r));
      }
    }

    return new FixedContextExecutor();
  }

  /**
   * Create an executor that propagates the {@link #current} context when {@link Executor#execute}
   * is called as the {@link #current} context of the {@code Runnable} scheduled. <em>Note that this
   * is a static method.</em>
   *
   * @see #fixedContextExecutor(Executor)
   */
  public static Executor currentContextExecutor(final Executor e) {
    final class CurrentContextExecutor implements Executor {
      @Override
      public void execute(Runnable r) {
        e.execute(Context.current().wrap(r));
      }
    }

    return new CurrentContextExecutor();
  }

  /** Lookup the value for a key in the context inheritance chain. */
  @Nullable
  Object lookup(Key<?> key) {
    return keyValueEntries.get(key);
  }

  /** Key for indexing values stored in a context. */
  public static final class Key<T> {
    private final String name;
    private final T defaultValue;

    Key(String name) {
      this(name, null);
    }

    Key(String name, T defaultValue) {
      this.name = checkNotNull(name, "name");
      this.defaultValue = defaultValue;
    }

    /** Get the value from the {@link #current()} context for this key. */
    @SuppressWarnings("unchecked")
    public T get() {
      return get(Context.current());
    }

    /** Get the value from the specified context for this key. */
    @SuppressWarnings("unchecked")
    public T get(Context context) {
      T value = (T) context.lookup(this);
      return value == null ? defaultValue : value;
    }

    @Override
    public String toString() {
      return name;
    }
  }

  private static <T> T checkNotNull(T reference, Object errorMessage) {
    if (reference == null) {
      throw new NullPointerException(String.valueOf(errorMessage));
    }
    return reference;
  }

  /**
   * If the ancestry chain length is unreasonably long, then print an error to the log and record
   * the stack trace.
   */
  private static void validateGeneration(int generation) {
    if (generation == CONTEXT_DEPTH_WARN_THRESH) {
      log.log(
          Level.SEVERE,
          "Context ancestry chain length is abnormally long. "
              + "This suggests an error in application code. "
              + "Length exceeded: "
              + CONTEXT_DEPTH_WARN_THRESH,
          new Exception());
    }
  }

  /**
   * Defines the mechanisms for attaching and detaching the "current" context. The constructor for
   * extending classes <em>must not</em> trigger any activity that can use Context, which includes
   * logging, otherwise it can trigger an infinite initialization loop. Extending classes must not
   * assume that only one instance will be created; Context guarantees it will only use one
   * instance, but it may create multiple and then throw away all but one.
   *
   * <p>The default implementation will put the current context in a {@link ThreadLocal}. If an
   * alternative implementation named {@code io.grpc.override.ContextStorageOverride} exists in the
   * classpath, it will be used instead of the default implementation.
   *
   * <p>This API is <a href="https://github.com/grpc/grpc-java/issues/2462">experimental</a> and
   * subject to change.
   */
  public abstract static class Storage {
    /**
     * Implements {@link Context#attach}.
     *
     * <p>Caution: {@link Context#attach()} interprets a return value of {@code null} to mean the
     * same thing as {@link Context#ROOT}.
     *
     * <p>See also: {@link #current()}.
     *
     * @param toAttach the context to be attached
     * @return A {@link Context} that should be passed back into {@link #detach(Context, Context)}
     *     as the {@code toRestore} parameter. {@code null} is a valid return value, but see caution
     *     note.
     */
    public abstract Context doAttach(Context toAttach);

    /**
     * Implements {@link Context#detach}.
     *
     * @param toDetach the context to be detached. Should be, or be equivalent to, the current
     *     context of the current scope
     * @param toRestore the context to be the current. Should be, or be equivalent to, the context
     *     of the outer scope
     */
    public abstract void detach(Context toDetach, Context toRestore);

    /**
     * Implements {@link Context#current}.
     *
     * <p>Caution: {@link Context} interprets a return value of {@code null} to mean the same thing
     * as {@code Context{@link Context#ROOT}}.
     *
     * <p>See also {@link #doAttach(Context)}.
     *
     * @return The context of the current scope. {@code null} is a valid return value, but see
     *     caution note.
     */
    public abstract Context current();
  }

  /** Creates a new context which propagates the values of this context. */
  Context fork() {
    return new Context(this, keyValueEntries);
  }
}
