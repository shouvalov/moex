package ru.altravita.moex

import com.opencsv.CSVWriter
import io.circe.{Decoder, Json}
import scopt.OptionParser

import java.io._
import java.net._
import java.text.NumberFormat
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.stream.Collectors
import scala.collection.JavaConverters._

object App {

  /** Описание API Moscow Exchange:
    * https://www.moex.com/a2193
    * https://fs.moex.com/files/6523
    */

  /** Число строк, которое moex-сервер должен отдавать в запросе.
    * Допустимые значения - 100, 50, 20, 10, 5, 1. *
    */
  final val limit = 100

  /** Максимальное число запросов к серверу для получения данных по одной акции.
    * Сервер отдаёт данные постранично. В запросе указываются параметры
    *   limit (максимальное число строк в ответе)
    *   start (с какой строки возвращать данные)
    */
  final val maxQueryCount = 100

  // format: off
//  final private val secIds = Seq(
//    "SBER", "GAZP", "LKOH", "GMKN", "CHMF", "ROSN", "TATN", "MGNT", "PLZL", "YNDX", "NLMK", "POLY", "MOEX", "MAGN",
//    "AFKS", "NVTK", "VTBR", "SBERP", "IRAO", "AFLT", "SNGSP", "DSKY", "ALRS", "MTSS", "FIVE", "SNGS", "MAIL"
//  ).take(displaySecs)
  // format: on

  final private val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")

  // Структуры, описывающие json-данные для исторических котировок
  case class HistoryPrice(date: LocalDate, secId: String, price: BigDecimal)

  /**  json выглядит так:
    *  {
    *  "history": {
    *    "columns": ["TRADEDATE", "SECID", "LEGALCLOSEPRICE"],
    *    "data": [
    *      ["2020-11-30", "SBER", 249.65],
    *      ["2020-12-01", "SBER", 259.76],
    *      ["2020-12-02", "SBER", 264.99]
    *    ]
    *  }}
    */
  implicit val decodeHistory: Decoder[Seq[HistoryPrice]] = c => {
    for {
      tuples <- c.downField("history").downField("data").as[Seq[(String, String, BigDecimal)]]
    } yield {
      tuples.map { case (dateStr, secId, closePrice) => HistoryPrice(LocalDate.parse(dateStr, dateFormatter), secId, closePrice) }
    }
  }

  // Структуры, описывающие json-данные для текущих котировок
  case class CurrentPrice(priceUpdateTime: String, secId: String, price: BigDecimal)

  /**  json выглядит так:
    *  {
    *  "marketdata": {
    *    "columns": ["UPDATETIME", "SECID", "LAST"],
    *    "data": [
    *      ["12:35:03", "CHMF", 1274.4],
    *      ["12:35:03", "GAZP", 201.99],
    *      ["12:35:03", "GMKN", 23534]
    *    ]
    *  }}
    */
  implicit val decodeCurrent: Decoder[Seq[CurrentPrice]] = c => {
    for {
      tuples <- c.downField("marketdata").downField("data").as[Seq[(String, String, BigDecimal)]]
    } yield {
      tuples.map { case (timeStr, secId, closePrice) => CurrentPrice(timeStr, secId, closePrice) }
    }
  }

  case class Candle(begin: String, end: String, priceOpen: BigDecimal, priceClose: BigDecimal)

  /**  json выглядит так:
    *      {
    *      "candles": {
    *        "metadata": {
    *          "begin": {"type": "datetime", "bytes": 19, "max_size": 0},
    *          "end": {"type": "datetime", "bytes": 19, "max_size": 0},
    *          "open": {"type": "double"},
    *          "close": {"type": "double"}
    *        },
    *        "columns": ["begin", "end", "open", "close"],
    *        "data": [
    *          ["2020-12-22 09:50:00", "2020-12-22 09:59:59", 210, 210],
    *          ["2020-12-22 10:00:00", "2020-12-22 10:09:59", 210, 209.8],
    *          ["2020-12-22 10:10:00", "2020-12-22 10:19:59", 209.8, 210.2],
    *          ["2020-12-22 10:20:00", "2020-12-22 10:29:59", 210.16, 209.84],
    *          ["2020-12-22 10:30:00", "2020-12-22 10:39:59", 209.98, 209.98]
    *        ]
    *      }}
    */
  implicit val decodeCandles: Decoder[Seq[Candle]] = c => {
    for {
      tuple <- c.downField("candles").downField("data").as[Seq[(String, String, BigDecimal, BigDecimal)]]
    } yield {
      tuple.map { case (beginStr, endStr, openPrice, closePrice) => Candle(beginStr, endStr, openPrice, closePrice) }
    }
  }

  def download(url: String): Json = {
    val strJson = new BufferedReader(new InputStreamReader(new URL(url).openStream)).lines().collect(Collectors.joining("\n"))

    io.circe.parser.parse(strJson) match {
      case Left(failure) => throw failure
      case Right(json)   => json
    }
  }

  def formatPrice(price: BigDecimal): String =
    //NumberFormat.getNumberInstance.format(price)
    price.toString()

  /** Возвращает список цен на акцию по датам в виде Map( дата -> цена ) */
  def getArchivePrices(secId: String, from: LocalDate, till: LocalDate): Map[ /*date:*/ LocalDate, /*price:*/ BigDecimal] = {
    val pages = (0 until maxQueryCount).view
      .map { start =>
        val url =
          s"http://iss.moex.com/iss/history/engines/stock/markets/shares/boards/TQBR/securities/${URLEncoder.encode(secId, "UTF-8")}.json" +
            s"?from=${from.format(dateFormatter)}" +
            s"&till=${till.format(dateFormatter)}" +
            s"&iss.meta=off&history.columns=TRADEDATE,SECID,LEGALCLOSEPRICE" +
            s"&limit=$limit" +
            s"&start=${limit * start}"

        println(url)

        val json = download(url)

        val historyPricesPage = json.hcursor.as[Seq[HistoryPrice]] match {
          case Left(failure) => throw failure
          case Right(value)  => value
        }

        historyPricesPage
      }
      .takeWhile(page => page.nonEmpty) // Если запрос вернул менее 'limit' строк, значит следующий запрос вернёт 0 строк и выполнять его нет смысла.

    pages.flatMap(historyPage => historyPage.map(row => row.date -> row.price)).toMap
  }

  /** Возвращает список текущих цен на акции */
  def getCurrentPrices(secIds: Set[String]): Map[ /*secId:*/ String, CurrentPrice] = {
    val url =
      s"http://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities.json" +
        s"?securities=${secIds.mkString(",")}" +
        s"&iss.only=marketdata" +
        s"&iss.dp=comma&iss.meta=off" +
        s"&marketdata.columns=UPDATETIME,SECID,LAST"

    println(url)

    val json = download(url)

    val market = json.hcursor.as[Seq[CurrentPrice]] match {
      case Left(failure) => throw failure
      case Right(value)  => value
    }

    market.map(row => row.secId -> row).toMap
  }

  /** Список доступных значений для interval (временное разрешение) может быть получен здесь:
    * http://iss.moex.com/iss/engines/stock/markets/shares/boards/tqbr/securities/NLMK/candleborders, где NLMK - код акции.
    * По умолчанию 10 минут.
    */
  def getCandles(secId: String, from: LocalDate, till: LocalDate): Seq[Candle] = {
    val pages = (0 until maxQueryCount).view
      .map { start =>
        val url =
          s"http://iss.moex.com/iss/engines/stock/markets/shares/boards/tqbr/securities/${URLEncoder.encode(secId, "UTF-8")}/candles.json" +
            s"?from=${from.format(dateFormatter)}" +
            s"?till=${till.format(dateFormatter)}" +
            s"&interval=10" +
            s"&candles.columns=begin,end,open,close" +
            s"&limit=$limit" +
            s"&start=${limit * start}"

        println(url)

        val json = download(url)

        val candles = json.hcursor.as[Seq[Candle]] match {
          case Left(failure) => throw failure
          case Right(value)  => value
        }

        candles
      }
      .takeWhile(page => page.nonEmpty) // Если запрос вернул менее 'limit' строк, значит следующий запрос вернёт 0 строк и выполнять его нет смысла.

    pages.flatten.toSeq
  }

  /** Вычисляет цену закрытия сегодняшнего дня.
    * Для этого в списке "свечей" сегодняшнего дня ищется запись на 18:40.
    */
  def getTodayClosePrice(secId: String): Option[BigDecimal] =
    getCandles(secId, LocalDate.now, LocalDate.now).find(candle => candle.begin.endsWith("18:40:00")).map(_.priceOpen)

  def getClosePrices(from: LocalDate, till: LocalDate, tickers: Seq[String], noDataStr: String): Seq[Seq[String]] = {
    // Строка с заголовком
    val header: Seq[String] = Seq("Date") ++ tickers

    // Исторические цены в виде прямоугольной таблицы. Первый столбец - дата, остальные столбцы - цены.
    val historicalPrices: Seq[Seq[String]] =
      if (from isBefore LocalDate.now) {
        // Исторические цены на акции
        val historicalData: Map[ /*secId:*/ String, Map[ /*date:*/ LocalDate, /*price:*/ BigDecimal]] =
          tickers.distinct.map(secId => secId -> getArchivePrices(secId, from, till)).toMap

        // Даты, на которые имеются исторические данные
        val dates = historicalData.flatMap { case (_ /*secId*/, prices) => prices.keys }.toSeq.distinct.sortWith(_ isBefore _)

        dates.map { date =>
          val dateColumn = Seq(date.format(dateFormatter))

          val priceColumns = tickers.map { secId =>
            val prices = historicalData(secId)
            prices.get(date).map(formatPrice).getOrElse(noDataStr)
          }

          dateColumn ++ priceColumns
        }
      } else
        Seq.empty

    // Цены закрытия сегодняшнего дня
    val todayClosePrices: Seq[String] =
      if (/* from <= today && till >= today : */ !(from isAfter LocalDate.now) && !(till isBefore LocalDate.now)) {
        val dateColumn = Seq(LocalDate.now.format(dateFormatter))

        val priceColumns = tickers.map { secId =>
          getTodayClosePrice(secId).map(formatPrice).getOrElse(noDataStr)
        }

        dateColumn ++ priceColumns
      } else
        Seq.empty

    // Прямоугольная таблица с данными для печати
    Seq(header) ++ historicalPrices ++ { if (todayClosePrices.nonEmpty) Seq(todayClosePrices) else Seq.empty }
  }

  def writeFile(path: String, data: Seq[Seq[String]]): Unit = {
    val writer = new CSVWriter(new FileWriter(path), '\t', '"', '"', "\r\n")
    writer.writeAll(data.map(_.toArray).asJava)
    writer.close()
  }

  def main(args: Array[String]): Unit = {
    case class Config(
        from: LocalDate = LocalDate.now().minusMonths(1),
        till: LocalDate = LocalDate.now(),
        tickers: Seq[String] = Seq("SBER", "GAZP", "YNDX"),
        outputFileName: String = "",
        noDataStr: String = "-",
        silent: Boolean = false
    )

    val parser = new OptionParser[Config]("moex-import") {
      head("moex-import", "0.1")
      opt[String]("from") required () action { (x, c) =>
        c.copy(from = LocalDate.parse(x, dateFormatter))
      } text "date in ISO 8601 format, YYYY-MM-DD. Required."
      opt[String]("till") action { (x, c) =>
        c.copy(till = LocalDate.parse(x, dateFormatter))
      } text "date in ISO 8601 format, YYYY-MM-DD. Default value is today."
      opt[Seq[String]]("tickers") unbounded () action { (x, c) =>
        c.copy(tickers = x)
      } text "Moscow Exchange ticker codes (SBER,GAZP,YNDX,...)"
      opt[String]("output-file") action { (x, c) =>
        c.copy(outputFileName = x)
      } text "output file name"
      opt[String]("no-data-string") action { (x, c) =>
        c.copy(noDataStr = x)
      } text "output file name"
      opt[String]("silent") action { (x, c) =>
        c.copy(outputFileName = x)
      } text "no output on display if specified"
    }

    parser.parse(args, Config()) foreach { conf =>
      val prices = getClosePrices(conf.from, conf.till, conf.tickers, conf.noDataStr)

      if (!conf.silent) {
        print(Tabulator.format(prices))
        println
      }

      if (conf.outputFileName.nonEmpty) {
        writeFile(conf.outputFileName, prices)
      }
    }
  }
}

object Tabulator {
  def format(table: Seq[Seq[Any]]): String = table match {
    case Seq() => ""
    case _ =>
      val sizes    = for (row <- table) yield for (cell <- row) yield if (cell == null) 0 else cell.toString.length
      val colSizes = for (col <- sizes.transpose) yield col.max
      val rows     = for (row <- table) yield formatRow(row, colSizes)
      formatRows(rowSeparator(colSizes), rows)
  }

  def formatRows(rowSeparator: String, rows: Seq[String]): String =
    (rowSeparator ::
      rows.head ::
      rowSeparator ::
      rows.tail.toList :::
      rowSeparator ::
      List()).mkString("\n")

  def formatRow(row: Seq[Any], colSizes: Seq[Int]): String = {
    val cells = for ((item, size) <- row.zip(colSizes)) yield if (size == 0) "" else ("%" + size + "s").format(item)
    cells.mkString("|", "|", "|")
  }

  def rowSeparator(colSizes: Seq[Int]): String = colSizes map { "-" * _ } mkString ("+", "+", "+")
}
