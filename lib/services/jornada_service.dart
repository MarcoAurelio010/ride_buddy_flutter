import 'dart:async';
import 'package:cloud_firestore/cloud_firestore.dart';
import 'package:geolocator/geolocator.dart';
import 'package:google_maps_flutter/google_maps_flutter.dart';
import 'package:flutter_background_service/flutter_background_service.dart';
import 'package:flutter_local_notifications/flutter_local_notifications.dart';
import 'package:firebase_core/firebase_core.dart';
import 'package:ride_buddy_flutter/models/jornada.dart';
import 'package:ride_buddy_flutter/models/user_profile.dart';
import 'package:ride_buddy_flutter/services/user_service.dart';

const String notificationChannelId = 'ridetracking_foreground';
const String statusNotificationChannelId = 'jornada_status';

// --- CONTROLE DE ESTADO ---
class JornadaController {
  static final JornadaController _instance = JornadaController._internal();
  factory JornadaController() => _instance;
  JornadaController._internal();

  bool isRunning = false;
  bool isPaused = false;
  int seconds = 0;
  List<LatLng> routePoints = [];
  double distanceKm = 0.0;

  final StreamController<Map<String, dynamic>> _dataController =
      StreamController<Map<String, dynamic>>.broadcast();
  Stream<Map<String, dynamic>> get dataStream => _dataController.stream;

  void updateDistance(double newDistance, LatLng newPoint) {
    distanceKm = newDistance;
    routePoints.add(newPoint);
    // REMOVIDO: _dataController.add aqui causava o erro de buffer.
    // Deixamos o Timer de 1s cuidar da atualização da tela.
  }

  void updateData({
    double? distance,
    List<LatLng>? points,
    int? time,
    bool? isRunning,
    bool? isPaused,
  }) {
    if (distance != null) distanceKm = distance;
    if (points != null) routePoints = points;
    if (time != null) seconds = time;
    if (isRunning != null) this.isRunning = isRunning;
    if (isPaused != null) this.isPaused = isPaused;

    _dataController.add({
      'km': distanceKm,
      'time': seconds,
      'isRunning': this.isRunning,
      'isPaused': this.isPaused,
    });
  }

  void reset() {
    isRunning = false;
    isPaused = false;
    seconds = 0;
    routePoints = [];
    distanceKm = 0.0;
    _dataController.add({
      'km': 0.0,
      'time': 0,
      'isRunning': false,
      'isPaused': false,
    });
  }
}

// --- SERVIÇO PRINCIPAL ---
class JornadaService {
  final UserService _userService = UserService();
  final JornadaController _controller = JornadaController();

  String? get _currentUserId => _userService.currentUserId;

  // 🛑 CORREÇÃO: Inicia a escuta assim que a classe é instanciada na tela
  JornadaService() {
    _startDataListener();
  }

  void _startDataListener() {
    FlutterBackgroundService().on('update').listen((event) {
      if (event != null) {
        _controller.updateData(
          distance: event['km'],
          time: event['time'],
          isRunning: event['isRunning'],
          isPaused: event['isPaused'],
        );
      }
    });
  }

  Stream<Map<String, dynamic>> get dataStream => _controller.dataStream;

  static Future<void> initializeService() async {
    final service = FlutterBackgroundService();

    const AndroidNotificationChannel foregroundChannel = AndroidNotificationChannel(
      notificationChannelId,
      'Rastreamento de Jornada',
      description: 'Canal para serviço de rastreamento em segundo plano.',
      importance: Importance.low,
    );

    const AndroidNotificationChannel statusChannel = AndroidNotificationChannel(
      statusNotificationChannelId,
      'Notificações de Status',
      description: 'Notificações de status de pausa/execução.',
      importance: Importance.max,
    );

    final FlutterLocalNotificationsPlugin flutterLocalNotificationsPlugin =
        FlutterLocalNotificationsPlugin();

    await flutterLocalNotificationsPlugin
        .resolvePlatformSpecificImplementation<
            AndroidFlutterLocalNotificationsPlugin>()
        ?.createNotificationChannel(foregroundChannel);
    
    await flutterLocalNotificationsPlugin
        .resolvePlatformSpecificImplementation<
            AndroidFlutterLocalNotificationsPlugin>()
        ?.createNotificationChannel(statusChannel);

    await service.configure(
      androidConfiguration: AndroidConfiguration(
        onStart: onStartService,
        isForegroundMode: true,
        autoStart: false,
        notificationChannelId: notificationChannelId,
      ),
      iosConfiguration: IosConfiguration(
        onForeground: onStartService,
        onBackground: onIosBackground,
        autoStart: false,
      ),
    );
  }

  Future<void> startTracking() async {
    bool serviceEnabled = await Geolocator.isLocationServiceEnabled();
    if (!serviceEnabled) throw Exception("GPS desativado.");
    
    LocationPermission permission = await Geolocator.checkPermission();
    if (permission == LocationPermission.denied) {
      permission = await Geolocator.requestPermission();
    }
    if (permission != LocationPermission.whileInUse &&
        permission != LocationPermission.always) {
      throw Exception("Permissão de localização negada.");
    }

    final service = FlutterBackgroundService();
    if (!(await service.isRunning())) {
      await service.startService();
    }

    _controller.reset();
    _controller.isRunning = true;
    _controller.updateData(isRunning: true, isPaused: false);

    // Dispara o listener no background
    service.invoke("startGpsListener");
  }

  void pauseTracking() {
    _controller.updateData(isPaused: true);
    FlutterBackgroundService().invoke("setAsPaused");
  }

  void resumeTracking() {
    _controller.updateData(isPaused: false);
    FlutterBackgroundService().invoke("setAsRunning");
  }

  Future<Jornada> stopTracking() async {
    _controller.isRunning = false;
    
    final double dist = _controller.distanceKm;
    final int duracao = _controller.seconds;
    
    final profile = await _userService.getUserProfile();
    final double preco = profile.precoGasolinaAtual;
    final double kml = profile.kmPorLitro;

    final double gasto = (kml > 0 && preco > 0) ? (dist / kml) * preco : 0.0;

    return Jornada(
      id: '',
      dataFim: DateTime.now(),
      kmPercorrido: dist,
      duracaoSegundos: duracao,
      gastoGasolina: gasto,
      desgasteOleoKm: dist,
      desgastePneuKm: dist,
    );
  }

  Future<void> saveJornada(Jornada jornada, UserProfile profile) async {
    final userId = _currentUserId;
    if (userId == null) return;

    await FirebaseFirestore.instance
        .collection('users')
        .doc(userId)
        .collection('jornadas')
        .add(jornada.toMap());

    final novoKm = profile.kmAtual + jornada.kmPercorrido.toInt();
    await _userService.saveUserProfile(profile.copyWith(kmAtual: novoKm));

    FlutterBackgroundService().invoke("stopService");
    _controller.reset();
  }

  Jornada recalculateJornada({
    required Jornada jornadaBase,
    required double kmFinal,
    required UserProfile profile,
  }) {
    final double preco = profile.precoGasolinaAtual;
    final double kml = profile.kmPorLitro;
    final double gasto = (kml > 0 && preco > 0) ? (kmFinal / kml) * preco : 0.0;

    return jornadaBase.copyWith(
      kmPercorrido: kmFinal,
      gastoGasolina: gasto,
      desgasteOleoKm: kmFinal,
      desgastePneuKm: kmFinal,
    );
  }

  Stream<List<Jornada>> getJornadas() {
    final userId = _currentUserId;
    if (userId == null) return const Stream.empty();
    return FirebaseFirestore.instance
        .collection('users')
        .doc(userId)
        .collection('jornadas')
        .orderBy('dataFim', descending: true)
        .snapshots()
        .map((s) => s.docs.map((d) => Jornada.fromMap(d.data(), d.id)).toList());
  }

  Future<void> deleteJornada(String jornadaId) async {
    final userId = _currentUserId;
    if (userId == null) return;
    await FirebaseFirestore.instance
        .collection('users')
        .doc(userId)
        .collection('jornadas')
        .doc(jornadaId)
        .delete();
  }

  static String _formatTime(int totalSeconds) {
    final int seconds = totalSeconds % 60;
    final int minutes = (totalSeconds ~/ 60) % 60;
    final int hours = (totalSeconds ~/ 3600);
    return '${hours.toString().padLeft(2, '0')}:${minutes.toString().padLeft(2, '0')}:${seconds.toString().padLeft(2, '0')}';
  }

  static void _showStatusNotification(
      String status, FlutterLocalNotificationsPlugin plugin) {
    plugin.show(
      1,
      "Status da Jornada",
      "Rastreamento $status!",
      const NotificationDetails(
        android: AndroidNotificationDetails(
          statusNotificationChannelId,
          'Notificações de Status',
          importance: Importance.max,
          priority: Priority.high,
        ),
      ),
    );
  }

  @pragma('vm:entry-point')
  static Future<void> onStartService(ServiceInstance service) async {
    await Firebase.initializeApp();
    final serviceController = JornadaController();
    final androidService = service is AndroidServiceInstance ? service : null;
    final flutterLocalNotificationsPlugin = FlutterLocalNotificationsPlugin();

    serviceController.isRunning = true;
    serviceController.updateData(isRunning: true, isPaused: false);
    _showStatusNotification("Iniciado", flutterLocalNotificationsPlugin);

    service.on("setAsPaused").listen((event) {
      serviceController.updateData(isPaused: true);
      _showStatusNotification("Pausado", flutterLocalNotificationsPlugin);
    });

    service.on("setAsRunning").listen((event) {
      serviceController.updateData(isPaused: false);
      _showStatusNotification("Retomado", flutterLocalNotificationsPlugin);
    });

    service.on("stopService").listen((event) {
      serviceController.isRunning = false;
      service.stopSelf();
    });

    // TIMER: Envia dados para a UI a cada 1 segundo
    Timer.periodic(const Duration(seconds: 1), (timer) {
      if (!serviceController.isRunning) {
        timer.cancel();
        return;
      }

      if (!serviceController.isPaused) {
        serviceController.seconds++;
      }

      if (androidService != null) {
        androidService.setForegroundNotificationInfo(
          title: "Ride Buddy: Rastreamento Ativo",
          content: "Tempo: ${_formatTime(serviceController.seconds)} | KM: ${serviceController.distanceKm.toStringAsFixed(2)} km",
        );
      }

      service.invoke('update', {
        'km': serviceController.distanceKm,
        'time': serviceController.seconds,
        'isRunning': serviceController.isRunning,
        'isPaused': serviceController.isPaused,
      });
    });

    // LISTENER DO GPS: Só inicia quando a UI pede
    service.on("startGpsListener").listen((event) {
      Geolocator.getPositionStream(
        locationSettings: const LocationSettings(
          accuracy: LocationAccuracy.bestForNavigation,
          distanceFilter: 10,
        ),
      ).listen((Position position) {
        if (!serviceController.isRunning || serviceController.isPaused) return;

        final newPoint = LatLng(position.latitude, position.longitude);

        if (serviceController.routePoints.isNotEmpty) {
          final lastPoint = serviceController.routePoints.last;
          final distance = Geolocator.distanceBetween(
            lastPoint.latitude, lastPoint.longitude,
            newPoint.latitude, newPoint.longitude,
          );
          
          serviceController.updateDistance(
            serviceController.distanceKm + (distance / 1000),
            newPoint,
          );
        } else {
          serviceController.updateDistance(0.0, newPoint);
        }
      });
    });
  }

  @pragma('vm:entry-point')
  static bool onIosBackground(ServiceInstance service) => true;
}