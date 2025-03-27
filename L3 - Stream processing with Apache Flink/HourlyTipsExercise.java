package com.ververica.flinktraining.exercises.datastream_java.windows;
import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiFareSource;
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import org.apache.flink.api.java.tuple.Tuple3; 
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class HourlyTipsExercise extends ExerciseBase {

  public static void main(String[] args) throws Exception {

    // Чтение параметров из командной строки
    ParameterTool params = ParameterTool.fromArgs(args);
    // Получение пути к входному файлу с данными о плате за такси. Если параметр "input" не указан, используется путь по умолчанию.
    final String input = params.get("input", ExerciseBase.pathToFareData);
    // Максимальная задержка событий в секундах (события могут приходить не по порядку, но не более чем на maxEventDelay секунд)
    final int maxEventDelay = 60;
    // Фактор скорости обработки (события за 10 минут обрабатываются за 1 секунду). Используется для эмуляции потоковой обработки.
    final int servingSpeedFactor = 600;

    // Настройка окружения для потоковой обработки данных
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.setParallelism(ExerciseBase.parallelism);

    // Создание потока данных о плате за такси из источника данных
    DataStream<TaxiFare> fares = env.addSource(fareSourceOrTest(new TaxiFareSource(input, maxEventDelay, servingSpeedFactor)));

    // Расчет почасовых чаевых для каждого водителя
    DataStream<Tuple3<Long, Long, Float>> hourlyMax = fares
        // Группировка данных по идентификатору водителя
        .keyBy((TaxiFare fare) -> fare.driverId)
        // Определение временного окна размером в 1 час
        .timeWindow(Time.hours(1))
        // расчет суммы чаевых в каждом окне
        .process(new AddTips())
        // Определение временного окна размером в 1 час для всех водителей
        .timeWindowAll(Time.hours(1))
        // Поиск водителя с максимальной суммой чаевых в каждом окне
        .maxBy(2);

    printOrTest(hourlyMax);

    env.execute("Hourly Tips (java)");
  }

  // расчет суммы чаевых для каждого водителя в каждом временном окне
  public static class AddTips extends ProcessWindowFunction<
      TaxiFare, // Тип входных данных
      Tuple3<Long, Long, Float>, // Тип выходных данных
      Long, // Тип ключа (идентификатор водителя)
      TimeWindow> { // Тип временного окна

    @Override
    public void process(
        Long key, // Идентификатор водителя
        Context context, // Контекст окна
        Iterable<TaxiFare> fares, // Итератор по всем записям о плате за такси в текущем окне для данного водителя
        Collector<Tuple3<Long, Long, Float>> out) throws Exception { 

      // Переменная для хранения суммы чаевых
      Float sumOfTips = 0F;

      // Итерация по всем записям о плате за такси в текущем окне
      for (TaxiFare f : fares) {
        // Добавление суммы чаевых из текущей записи к общей сумме
        sumOfTips += f.tip;
      }

      out.collect(new Tuple3<>(context.window().getEnd(), key, sumOfTips));
    }
  }
}
