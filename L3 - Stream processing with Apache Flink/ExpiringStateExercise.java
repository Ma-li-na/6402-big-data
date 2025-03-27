package com.ververica.flinktraining.exercises.datastream_java.process;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiFareSource;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class ExpiringStateExercise extends ExerciseBase {

  // Тэг для потока поездок, для которых не найдена соответствующая плата
  static final OutputTag<TaxiRide> unmatchedRides = new OutputTag<TaxiRide>("unmatchedRides") {};
  // Тэг для потока плат, для которых не найдена соответствующая поездка
  static final OutputTag<TaxiFare> unmatchedFares = new OutputTag<TaxiFare>("unmatchedFares") {};

  public static void main(String[] args) throws Exception {

    // Получение параметров командной строки
    ParameterTool params = ParameterTool.fromArgs(args);
    // Получение пути к файлу с данными о поездках (если не указан, используется путь по умолчанию)
    final String ridesFile = params.get("rides", ExerciseBase.pathToRideData);
    // Получение пути к файлу с данными о плате за поездки (если не указан, используется путь по умолчанию)
    final String faresFile = params.get("fares", ExerciseBase.pathToFareData);

    final int maxEventDelay = 60;           // Максимальная задержка событий (в секундах)
    final int servingSpeedFactor = 600;   // Фактор скорости обработки (10 минут данных обрабатываются за секунду)

    // Настройка окружения для потоковой обработки
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    // Установка временной характеристики потока на EventTime 
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    // Установка количества параллельных задач
    env.setParallelism(ExerciseBase.parallelism);

    // Создание потока данных о поездках из файла
    DataStream<TaxiRide> rides = env
        .addSource(rideSourceOrTest(new TaxiRideSource(ridesFile, maxEventDelay, servingSpeedFactor)))
        // Фильтрация поездок: выбираем только начальные поездки и исключаем тестовые данные
        .filter((TaxiRide ride) -> (ride.isStart && (ride.rideId % 1000 != 0)))
        .keyBy(ride -> ride.rideId);

    // Создание потока данных о плате за поездки из файла
    DataStream<TaxiFare> fares = env
        .addSource(fareSourceOrTest(new TaxiFareSource(faresFile, maxEventDelay, servingSpeedFactor)))
        .keyBy(fare -> fare.rideId);

    // Соединение потоков поездок и платы и обработка 
    SingleOutputStreamOperator processed = rides
        .connect(fares)
        .process(new EnrichmentFunction());

    // Вывод в консоль (или тестирование) потока плат, для которых не найдена соответствующая поездка
    printOrTest(processed.getSideOutput(unmatchedFares));

    // Запуск задачи Flink с именем "ExpiringStateSolution (java)"
    env.execute("ExpiringStateSolution (java)");
  }

  // Функция обогащения данных (соединение поездок и платы)
  public static class EnrichmentFunction extends KeyedCoProcessFunction<Long, TaxiRide, TaxiFare, Tuple2<TaxiRide, TaxiFare>> {

    // Состояние для хранения поездки (ключевое, управляемое Flink)
    private ValueState<TaxiRide> rideState;
    // Состояние для хранения платы за поездку (ключевое, управляемое Flink)
    private ValueState<TaxiFare> fareState;

    @Override
    public void open(Configuration config) {
      // Инициализация состояния для хранения поездки
      rideState = getRuntimeContext().getState(new ValueStateDescriptor<>("saved ride", TaxiRide.class));
      // Инициализация состояния для хранения платы за поездку
      fareState = getRuntimeContext().getState(new ValueStateDescriptor<>("saved fare", TaxiFare.class));
    }

    @Override
    public void processElement1(TaxiRide ride, Context context, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
      // Получение платы из состояния (если она там есть)
      TaxiFare fare = fareState.value();
      if (fare != null) {
        // Если плата найдена, очищаем состояние
        fareState.clear();
        // Удаляем таймер, если он был установлен для этой платы
        context.timerService().deleteEventTimeTimer(fare.getEventTime());
        // Собираем (emit) пару (поездка, плата)
        out.collect(new Tuple2(ride, fare));
      } else {
        // Если плата не найдена, сохраняем поездку в состояние
        rideState.update(ride);
        // Регистрируем таймер на время события (чтобы потом обработать ситуацию, если плата не придет)
        // Как только приходит watermark, мы можем прекратить ожидание соответствующей платы
        context.timerService().registerEventTimeTimer(ride.getEventTime());
      }
    }
