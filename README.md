# Запус проекта

1. Установить docker и docker compose
2. Из корня проекта выполнить команду `docker compose up --build -d` 
3. После билда имаджей docker их запустит и проверит состояние. Пример состояния:
```
  ✔ Network recommendation_system_default                     Created                                                                                                                                                                                            0.1s
 ✔ Container recommendation_system-prometheus-1              Started                                                                                                                                                                                            0.1s
 ✔ Container recommendation_system-rabbitmq-1                Healthy                                                                                                                                                                                            0.1s
 ✔ Container recommendation_system-redis-1                   Healthy                                                                                                                                                                                            0.1s
 ✔ Container recommendation_system-grafana-1                 Started                                                                                                                                                                                            0.1s
 ✔ Container recommendation_system-recommendation_service-1  Started                                                                                                                                                                                            0.1s
 ✔ Container recommendation_system-event_collector-1         Started                                                                                                                                                                                            0.1s
 ✔ Container recommendation_system-regular_pipeline-1        Started
 ```
4. Сабмитим адрес вашей машины в задание
5. Логи всей сборки можно смотреть через `docker compose logs -f`

# Структура проекта
* event_collector - сервис для сбора событий, обрабатывает вызов `/interact` на порту 5000 и пишет события в очередь rabbitmq
* regular_pipeline - раз в 10 секунд читает события из редиса и записывает их в INTERACTIONS_FILE
    * recs.py - главный скрипт рекомендаций регулярно вычисляет рекомендации и записывает их в редис
* recommendations 
    * /healthcheck возвращает статус сервиса
    * /cleanup производит сброс окружения перед новым запуском грейдера
    * /add_items добавляет в систему новые объекты рекомендации 
    * /recs/{user_id} читает рекомендации из редиса, если нет - то возвращает рандомные, также в 5% случаев возваращает рандомные, даже если есть рекомендации (чтобы улучшить разнообразие)
* grafana - визуализация метрик, откройте в браузере http://адрес вашей виртуалки:3000/ и введите логин/пароль admin/admin, дальше найдите там recommendation-service-dashboard в дашбордах

