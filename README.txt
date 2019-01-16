Перед запуском кода следует создать таблицу для сырых данных и настроить строку соединения
в executeQuery().
Код таблицы:
CREATE TABLE public.buildings
(
    bid INT,
    address VARCHAR(200),
    created_year VARCHAR(40),
    max_floors VARCHAR(40),
    houses_qty VARCHAR(40),
    floor_type VARCHAR(40),
    wall_type VARCHAR(40),
    playground VARCHAR(40),
    cadastr VARCHAR(100),
    is_emergency VARCHAR(40),
    garbage_chute VARCHAR(40),
    living_area VARCHAR(40),
    latitude VARCHAR(40),
    longitude VARCHAR(40),
    link_to_website VARCHAR(200)
)