package akka

import com.ning.http.client.AsyncHandler.STATE
import com.ning.http.client.AsyncHandler
import com.ning.http.client.AsyncHttpClient
import com.ning.http.client.AsyncHttpClientConfig
import com.ning.http.client.HttpResponseBodyPart
import com.ning.http.client.HttpResponseHeaders
import com.ning.http.client.HttpResponseStatus
import com.ning.http.client.Response

import akka.actor.Actor.actorOf
import akka.actor.Actor
import akka.dispatch._
import akka.event.EventHandler
import akka.routing._

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.Executors

object FeedReader extends App {

  // nrOfFetchers limits active http requests (how badly do we want to
  // hose the server) while nrOfFeeds is total requests we'll make.
  // Note that AsyncHttpClient also has config options
  // that could affect things.
  read(nrOfFetchers = 20, nrOfFeeds = 1000)

  // ====================
  // ===== Messages =====
  // ====================
  sealed trait Message
  case object Read extends Message
  case class Fetch(url: String) extends Message
  case class CompleteFetch(url: String) extends Message

  // if this is a val it seems to end up as 0 ???
  def usefulCPUThreads = Runtime.getRuntime().availableProcessors() * 2

  object Feed {
    // This is a mock implementation. In production, it should connect
    // to a datastore / database.  we want several sample feeds to try
    // to keep "max connections per host" in AsyncHttpClient from
    // becoming a limiting factor.
    private val mockFeeds = Feed("http://feeds.feedburner.com/TechCrunch") ::
    Feed("http://feeds.reuters.com/reuters/topNews") ::
    Feed("http://rss.cnn.com/rss/cnn_topstories.rss") ::
    Feed("http://www.nytimes.com/services/xml/rss/nyt/HomePage.xml") ::
    Nil
    def infiniteFeeds: Stream[Feed] = {
      def nextFeed(list: List[Feed]): Stream[Feed] = {
        list match {
          case head :: tail =>
            Stream.cons(head, nextFeed(tail))
          case Nil =>
            nextFeed(mockFeeds)
        }
      }
      nextFeed(Nil)
    }
  }

  case class Feed(val url: String)

  class Reader(nrOfFetchers: Int, nrOfFeeds: Int)
  extends Actor {
    var nrOfFetching: Int = _
    val pool = actorOf(new FetcherPool(nrOfFetchers))

    // Phase 1, can accept a Read message
    def fromClient: Receive = {
      case Read =>
        nrOfFetching = nrOfFeeds
        for (feed <- Feed.infiniteFeeds take nrOfFeeds) {
          pool ! Fetch(feed.url)
        }
    }

    // Phase 2, accept complete ack from the fetcher
    def fromFetcher: Receive = {
      case CompleteFetch(url) =>
        nrOfFetching -= 1

        EventHandler.info(this, "%d left %d in flight %d threads: %s".format(nrOfFetching, Fetcher.httpInFlight.get(), Thread.activeCount, url))

        // When the number of completed fetchers reach zero, shut down
        if (nrOfFetching == 0) {
          self.stop()

          System.exit(0)
        }
    }

    // From client or from fetchers
    def receive = fromClient orElse fromFetcher

    override def preStart() {
      pool.start()
    }

    override def postStop() {
      pool.stop()
    }
  }

  class FetcherPool(nrOfFetchers: Int)
  extends Actor
  with DefaultActorPool
  with BoundedCapacityStrategy
  with MailboxPressureCapacitor // overrides pressureThreshold based on mailboxes
  with SmallestMailboxSelector
  //with RunningMeanBackoff
  // With a backoff filter, the fetchers can get killed with an outstanding
  // http request still pending. Need a way to kill with a timeout or
  // remove from pool without killing or something. Don't backoff
  // for now.
  with BasicNoBackoffFilter
  with BasicRampup {
    // BoundedCapacitor min and max actors in pool.
    // should probably be configurable.
    override val lowerBound = 1
    override val upperBound = nrOfFetchers

    // these values are kinda random
    override val pressureThreshold = 1
    override val partialFill = true
    override val selectionCount = 1
    override val rampupRate = 0.1

    // all pool members share a work stealing dispatcher
    val childDispatcher =
      Dispatchers.newExecutorBasedEventDrivenWorkStealingDispatcher("pool")
      // must use synchronous (non-queueing) queue or max pool size
      // is ignored (core is effectively max if you have a queue)
      .withNewThreadPoolWithSynchronousQueueWithFairness(false)
      .setCorePoolSize(1)
      .setMaxPoolSize(usefulCPUThreads)
      .build

    // AsyncHttpClient is designed to be shared across
    // many connections, so we make it global here.
    // otherwise you end up with a whole lot of extra
    // threads and other overhead for each actor
    // in the pool.
    val httpClient = Fetcher.makeClient

    override def instance = {
      val actorRef = actorOf(new Fetcher(httpClient))
      actorRef.dispatcher = childDispatcher
      actorRef
    }

    override def receive = _route
  }

  object Fetcher {
    val httpInFlight = new AtomicInteger(0)

    def makeClient = {
      // if we aren't going to block we don't need an unbounded
      // executor, but we use an unbounded one here to
      // demonstrate that the rest of the setup is well-behaved
      // and won't swamp us in requests.
      val executor = Executors.newCachedThreadPool()

      val builder = new AsyncHttpClientConfig.Builder()
      val config = builder.setMaximumConnectionsTotal(1000)
        .setMaximumConnectionsPerHost(30)
        .setExecutorService(executor)
        .build
      new AsyncHttpClient(config)
    }
  }

  class Fetcher(client: AsyncHttpClient) extends Actor {

    def receive = {
      case Fetch(url) =>
        // Fetch the content
        val httpHandler = new AsyncHandler[Response]() {
          Fetcher.httpInFlight.incrementAndGet()

          val builder =
            new Response.ResponseBuilder()

          // copy this because self.channel won't be
          // the same when we're in the callbacks
          val replyChannel = self.channel

          def onThrowable(t: Throwable) {
            EventHandler.error(this, t.getMessage)
          }

          def onBodyPartReceived(bodyPart: HttpResponseBodyPart) = {
            builder.accumulate(bodyPart)
            STATE.CONTINUE
          }

          def onStatusReceived(responseStatus: HttpResponseStatus) = {
            STATE.CONTINUE
          }
          def onHeadersReceived(headers: HttpResponseHeaders) = {
            //EventHandler.info(this, "Content-Type: %s".format(headers.getHeaders.getFirstValue("Content-Type")))
            STATE.CONTINUE
          }

          def onCompleted() = {
            Fetcher.httpInFlight.decrementAndGet()

            val response = builder.build()
            replyChannel ! CompleteFetch(url)

            // we can take new requests now
            self.dispatcher.resume(self)

            response
          }
        }

      // don't start a new request with this actor
      // until we finish the previous one
      self.dispatcher.suspend(self)

      client.prepareGet(url).execute(httpHandler)
    }
  }

  def read(nrOfFetchers: Int, nrOfFeeds: Int) {
    // create the reader
    val reader = actorOf(new Reader(nrOfFetchers, nrOfFeeds)).start()

    //send Read message
    reader ! Read
  }
}
