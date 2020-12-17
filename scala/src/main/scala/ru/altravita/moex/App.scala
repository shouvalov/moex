package ru.altravita.moex

import com.opencsv.CSVWriter
import io.circe.{Decoder, Json}

import java.io._
import java.net._
import java.text.NumberFormat
import java.time.{LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter
import java.util.stream.Collectors
import scala.collection.JavaConverters._

object App {
  final private val displayDays    = 20
  final private val displaySecs    = 999
  final private val outputFileName = s"moex-${LocalDateTime.now.format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss"))}.csv"

  // format: off
  final private val secIds = Seq(
    "SBER", "GAZP", "LKOH", "GMKN", "CHMF", "ROSN", "TATN", "MGNT", "PLZL", "YNDX", "NLMK", "POLY", "MOEX", "MAGN",
    "AFKS", "NVTK", "VTBR", "SBERP", "IRAO", "AFLT", "SNGSP", "DSKY", "ALRS", "MTSS", "FIVE", "SNGS", "MAIL"
  ).take(displaySecs)
  // format: on

  final private val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")

  // Структуры, описывающие json-данные для исторических котировок
  case class History(columns: Seq[String], data: Seq[HistoryData])

  case class HistoryData(date: LocalDate, secId: String, price: BigDecimal)

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
  implicit val decodeHistory: Decoder[History] = c => {
    for {
      columns <- c.downField("history").downField("columns").as[Seq[String]]
      tuple   <- c.downField("history").downField("data").as[Seq[(String, String, BigDecimal)]]
    } yield {
      val data = tuple.map { case (dateStr, secId, closePrice) => HistoryData(LocalDate.parse(dateStr, dateFormatter), secId, closePrice) }
      History(columns, data)
    }
  }

  // Структуры, описывающие json-данные для текущих котировок
  case class Market(columns: Seq[String], data: Seq[MarketData])

  case class MarketData(priceUpdateTime: String, secId: String, price: BigDecimal)

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
  implicit val decodeMarket: Decoder[Market] = c => {
    for {
      columns <- c.downField("marketdata").downField("columns").as[Seq[String]]
      tuple   <- c.downField("marketdata").downField("data").as[Seq[(String, String, BigDecimal)]]
    } yield {
      val data = tuple.map { case (timeStr, secId, closePrice) => MarketData(timeStr, secId, closePrice) }
      Market(columns, data)
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
    NumberFormat.getNumberInstance.format(price)

  /** Возвращает список цен на акцию по датам в виде Map( дата -> цена ) */
  def getArchivePrices(secId: String, from: LocalDate, till: LocalDate): Map[ /*date:*/ LocalDate, /*price:*/ BigDecimal] = {
    val url =
      s"http://iss.moex.com/iss/history/engines/stock/markets/shares/boards/TQBR/securities/$secId.json" +
        s"?from=${from.format(dateFormatter)}" +
        s"&till=${till.format(dateFormatter)}" +
        s"&iss.meta=off&history.columns=TRADEDATE,SECID,LEGALCLOSEPRICE"

    val json = download(url)

    val history = json.hcursor.as[History] match {
      case Left(failure) => throw failure
      case Right(value)  => value
    }

    history.data.map(row => row.date -> row.price).toMap
  }

  case class CurrentPriceInfo(updateTime: String, price: BigDecimal)

  /** Возвращает список текущих цен на акцию */
  def getCurrentPrices(secIds: Seq[String]): Map[ /*secId:*/ String, CurrentPriceInfo] = {
    val url =
      s"http://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities.json" +
        s"?securities=${secIds.mkString(",")}" +
        s"&iss.only=marketdata" +
        s"&iss.dp=comma&iss.meta=off" +
        s"&marketdata.columns=UPDATETIME,SECID,LAST"

    val json = download(url)

    val market = json.hcursor.as[Market] match {
      case Left(failure) => throw failure
      case Right(value)  => value
    }

    market.data.map(row => row.secId -> CurrentPriceInfo(row.priceUpdateTime, row.price)).toMap
  }

  def writeFile(path: String, data: Seq[Seq[String]]): Unit = {
    val writer = new CSVWriter(new FileWriter(path), '\t', '"', '"', "\r\n")
    writer.writeAll(data.map(_.toArray).asJava)
    writer.close()
  }

  def main(args: Array[String]): Unit = {
    val from = LocalDate.now().minusDays(displayDays)
    val till = LocalDate.now()

    // Исторические цены на акции
    val historicalData: Map[ /*secId:*/ String, Map[ /*date:*/ LocalDate, /*price:*/ BigDecimal]] =
      secIds.map(secId => secId -> getArchivePrices(secId, from, till)).toMap

    // Текущие цены
    val currentPrices: Map[ /*secId:*/ String, CurrentPriceInfo] = getCurrentPrices(secIds)

    // Прямоугольная таблица с данными для печати
    val table: Seq[Seq[String]] = {
      val header: Seq[String] = Seq("Date") ++ secIds

      val history: Seq[Seq[String]] = {
        // Даты, на которые имеются исторические данные
        val dates = historicalData.flatMap { case (_ /*secId*/, prices) => prices.keys }.toSeq.distinct.sortWith(_ isBefore _)

        dates.map { date =>
          val dateColumn = Seq(date.toString)
          val priceColumns = secIds.map { secId =>
            val prices = historicalData(secId)
            prices.get(date).map(formatPrice).getOrElse("-")
          }

          dateColumn ++ priceColumns
        }
      }

      val actual: Seq[String] = {
        val dateColumn = Seq(LocalDate.now.toString)
        val priceColumns = secIds.map { secId =>
          val priceOpt = currentPrices.get(secId)
          priceOpt.map(currentPriceInfo => formatPrice(currentPriceInfo.price)).getOrElse("-")
        }

        dateColumn ++ priceColumns
      }

      Seq(header) ++ history ++ Seq(actual)
    }

    print(Tabulator.format(table))
    println

    writeFile(outputFileName, table)
  }
}

object Tabulator {
  def format(table: Seq[Seq[Any]]): String = table match {
    case Seq() => ""
    case _ =>
      val sizes    = for (row <- table) yield (for (cell <- row) yield if (cell == null) 0 else cell.toString.length)
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

//http://iss.moex.com/iss/history/engines/stock/markets/shares/boards/TQBR/securities/SBER?from=2020-12-14&till=2020-12-16&iss.meta=off&history.columns=TRADEDATE,SECID,LEGALCLOSEPRICE
