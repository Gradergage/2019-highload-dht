# Яндекс танк

#### 1. Лента с PUT с уникальными ключами
[line](https://overload.yandex.net/276915) |
[const](https://overload.yandex.net/276919)

#### 2. Лента с PUT с частичной перезаписью ключей (вероятность 10%)
[line](https://overload.yandex.net/276923) |
[const](https://overload.yandex.net/276924)


#### 3. Лента с GET существующих ключей с равномерным распределением (стреляем по наполненной БД)
[line](https://overload.yandex.net/276926) |
[const](https://overload.yandex.net/276929)

#### 4. Лента со смещением распределения GET к недавно добавленным ключам (частый случай на практике)
Смещение распределения выполняется следующим образом: 20% запросов будут от 0 до 90% по временной шкале put-ов, 80% запросов будут от 90% до 100% по временной шкале put-ов.
 
[line](https://overload.yandex.net/276930) |
[const](https://overload.yandex.net/276933)

#### 5. Лента со смешанной нагрузкой с 50% PUT новых ключей и 50% GET существующих ключей (равномерное распределение)
[line](https://overload.yandex.net/276936) |
[const](https://overload.yandex.net/276935)