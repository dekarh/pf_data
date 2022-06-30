# Выгрузка всех значений всех полей с учетом справочников и внутренних списков ПФ в единую реляционную БД,
# состоящую из 5 таблиц с внешними ключами (ограничениями) справочников и внутренних списков

import json
import os
import requests
import xmltodict
from lxml import etree, objectify
import csv
from datetime import datetime, timedelta
import sqlite3

from lib import format_phone, l, fl
from hide_data import USR_Tocken, PSR_Tocken, PF_ACCOUNT, DEPARTMENTS, OFFICETOWNS, TYPES, FIELDS, LISTS
from api2backup import reload_all, api_load_from_list

BACKUP_DIRECTORY = 'current'
PF_DATA = '../data'
DOCFLOW = '../../docflow/data'
CREATE_DB = True


def printProgressBar (iteration, total, prefix = '', suffix = '', decimals = 1, length = 100, fill = '█'):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        length      - Optional  : character length of bar (Int)
        fill        - Optional  : bar fill character (Str)
    """
    if total == 0:
        total = 1
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    print('\r%s |%s| %s%% %s' % (prefix, bar, percent, suffix), end = '\r')
    # Print New Line on Complete
    if iteration == total:
        print()

def extract_lists_from_pf(xml_parts, var1_name, var2_name='', var3_name='', level_name=''):
    """ Выгрузка списков из xml-структуры ПФ"""
    var1 = []
    var2 = []
    var3 = []
    if str(type(xml_parts)).replace("'", '') == '<class list>':
        for xml_part in xml_parts:
            if level_name:
                if xml_part[level_name].get(var1_name, False) != False:
                    var1.append(xml_part[level_name][var1_name])
                else:
                    var1.append(None)
                if var2_name:
                    if xml_part[level_name].get(var2_name, False) != False:
                        var2.append(xml_part[level_name][var2_name])
                    else:
                        var2.append(None)
                if var3_name:
                    if xml_part[level_name].get(var3_name, False) != False:
                        var3.append(xml_part[level_name][var3_name])
                    else:
                        var3.append(None)
            else:
                if xml_part.get(var1_name, False) != False:
                    var1.append(xml_part[var1_name])
                else:
                    var1.append(None)
                if var2_name:
                    if xml_part.get(var2_name, False) != False:
                        var2.append(xml_part[var2_name])
                    else:
                        var2.append(None)
                if var3_name:
                    if xml_part.get(var3_name, False) != False:
                        var3.append(xml_part[var3_name])
                    else:
                        var3.append(None)
        return var1, var2, var3
    else:
        if level_name:
            if xml_parts.get(level_name, False) != False:
                if xml_parts[level_name].get(var1_name, False) != False:
                    var1.append(xml_parts[level_name][var1_name])
                else:
                    var1.append(None)
                    if var2_name:
                        if xml_parts[level_name].get(var2_name, False) != False:
                            var2.append(xml_parts[level_name][var2_name])
                        else:
                            var2.append(None)
                    if var3_name:
                        if xml_parts[level_name].get(var3_name, False) != False:
                            var3.append(xml_parts[level_name][var3_name])
                        else:
                            var3.append(None)
                    return var1, var2, var3
        else:
            if xml_parts.get(var1_name, False) != False:
                var1.append(xml_parts[var1_name])
            else:
                var1.append(None)
                if var2_name:
                    if xml_parts.get(var2_name, False) != False:
                        var2.append(xml_parts[var2_name])
                    else:
                        var2.append(None)
                if var3_name:
                    if xml_parts.get(var3_name, False) != False:
                        var3.append(xml_parts[var3_name])
                    else:
                        var3.append(None)
                return var1, var2, var3
    return [], [], []

if __name__ == "__main__":
    # Загружаем шаблоны задач
    tasktemplates = {}
    with open(os.path.join(BACKUP_DIRECTORY, 'tasktemplates_full.json'), 'r') as read_file:
        loaded_tasktemplates = json.load(read_file)
    for tasktemplate in loaded_tasktemplates:
        tasktemplates[int(tasktemplate)] = loaded_tasktemplates[tasktemplate]
    # Загружаем справочники
    with open(os.path.join(BACKUP_DIRECTORY, 'handbooks_full.json'), 'r') as read_file:
        handsbooks_loaded = json.load(read_file)
    handsbooks = {}
    for handsbook in handsbooks_loaded:
        handsbooks[int(handsbook)] = handsbooks_loaded[handsbook]
    # Загружаем задачи
    with open(os.path.join(BACKUP_DIRECTORY, 'tasks_full.json'), 'r') as read_file:
        tasks_loaded = json.load(read_file)
    tasks = {}
    for task in tasks_loaded:
        tasks[int(task)] = tasks_loaded[task]

    if CREATE_DB:
        if os.path.exists(os.path.join(BACKUP_DIRECTORY, 'fields.db')):
            os.remove(os.path.join(BACKUP_DIRECTORY, 'fields.db'))
    conn = sqlite3.connect(os.path.join(BACKUP_DIRECTORY, 'fields.db'))
    cur = conn.cursor()

    if CREATE_DB:
        cur.execute("""
        PRAGMA foreign_keys=on;""")
        cur.execute("""
        CREATE TABLE IF NOT EXISTS types(
            id INT PRIMARY KEY,
            typename TEXT NOT NULL,
            column_names TEXT NOT NULL,
            column_ids TEXT NOT NULL);""")
        conn.commit()
        cur.execute("""            
        CREATE TABLE IF NOT EXISTS fields(
            id INT PRIMARY KEY,
            title TEXT NOT NULL,
            type_id INT NOT NULL,
            FOREIGN KEY (type_id) REFERENCES types(id));""")
        conn.commit()
        cur.execute("""       
        CREATE TABLE IF NOT EXISTS amounts(
            task_id INT NOT NULL,
            real_value REAL,
            int_value INT,
            text_value TEXT NOT NULL,
            datetime_value DATETIME,
            timedelta_value INT,
            values_list TEXT,
            list_id INT NOT NULL,
            type_id INT NOT NULL,
            field_id INT NOT NULL,
            CONSTRAINT id PRIMARY KEY (task_id, field_id),
            FOREIGN KEY (field_id) REFERENCES fields(id),              
            FOREIGN KEY (type_id) REFERENCES types(id),
            FOREIGN KEY (list_id, type_id) REFERENCES lists(list_id, type_id));""")
        #
        conn.commit()
        cur.execute("""       
        CREATE TABLE IF NOT EXISTS lists(
            list_id INT NOT NULL,
            type_id INT NOT NULL,
            real_value REAL,
            int_value INT,
            text_value TEXT NOT NULL,
            values_list TEXT,
            CONSTRAINT id PRIMARY KEY (type_id, list_id),
            FOREIGN KEY (type_id) REFERENCES types(id));""")
        conn.commit()
        cur.execute("""       
        CREATE TABLE IF NOT EXISTS arrays(
            task_id INT NOT NULL,
            field_id INT NOT NULL,
            list_id INT NOT NULL,
            type_id INT NOT NULL,
            CONSTRAINT id PRIMARY KEY (task_id, field_id, list_id, type_id),       
            FOREIGN KEY (task_id, field_id) REFERENCES amounts(task_id, field_id),         
            FOREIGN KEY (list_id, type_id) REFERENCES lists(list_id, type_id));""")
        conn.commit()
        cur.executemany('INSERT INTO types VALUES(?, ?, ?, ?);', TYPES)
        conn.commit()
        cur.executemany('INSERT INTO fields VALUES(?, ?, ?);', FIELDS)
        conn.commit()
        cur.executemany('INSERT INTO lists VALUES(?, ?, ?, ?, ?, ?);', LISTS)
        conn.commit()

        lists = []
        # Загружаем справочники из json
        for handsbook1000 in [1002,1006,1014,1018,1022,1032,1034,1038,1040,1062,1064,1066,1070,1084,1086,1088,1090,
                              1092,1094,1096,1098]:
            handsbook = handsbook1000 - 1000
            cur.execute('SELECT column_ids FROM types WHERE id = ?;', (handsbook + 1000,))
            request = cur.fetchone()
            if str(type(request)).replace("'", '') == '<class tuple>':
                field_ids = json.loads(request[0])
            else:
                fields_ids = []
            i = 0
            if handsbook1000 in [1006, 1040, 1066, 1064, 1070, 1032, 1090, 1092]:
                # Cправочники - кандидаты на перевод в простые списки
                for record in handsbooks[handsbook]:
                    if len(handsbooks[handsbook][record]) != 1 \
                            and not (len(handsbooks[handsbook][record]) > 1 and len(handsbooks[handsbook][record]) < 3
                                     and 0 in field_ids):
                        print('Слишком большое количество полей в записи БД!!!!', handsbook, record)
                    else:
                        filled = False
                        for field in handsbooks[handsbook][record]:
                            if field:
                                i += 1
                                lists.append((
                                    int(record),
                                    handsbook1000,
                                    None,
                                    None,
                                    handsbooks[handsbook][record][field]['text'],
                                    None
                                ))
                                filled = True
                        if not filled:
                            i += 1
                            lists.append((
                                int(record),
                                handsbook1000,
                                None,
                                None,
                                handsbooks[handsbook][record][handsbooks[handsbook][record].keys()[0]]['text'],
                                None
                            ))
            else:
                # Cправочники - кандидаты на перевод в модели
                for record in handsbooks[handsbook]:
                    i += 1
                    fields_list = []
                    values_list = []
                    for field in field_ids:
                        fields_list.append(handsbooks[handsbook][record][str(field)]['text'])
                        values_list.append(handsbooks[handsbook][record][str(field)]['value'])
                    lists.append((
                        int(record),
                        handsbook1000,
                        None,
                        None,
                        json.dumps(fields_list, ensure_ascii=False),
                        json.dumps(values_list, ensure_ascii=False),
                    ))

        # Загружаем справочники из задач
        i201 = 0
        i202 = 0
        i203 = 0
        i204 = 0
        for task in tasks:
            template_id = ''
            if tasks[task].get('template', None):
                if tasks[task]['template'].get('id', None):
                    template_id = tasks[task]['template']['id']
            if template_id == '18120484':       # Справочник ЦФО - 201
                i201 += 1
                workers_ids = []
                workers_names = []
                if tasks[task].get('workers', None):
                    if tasks[task]['workers'].get('groups', None):
                        if tasks[task]['workers']['groups'].get('group', None):
                            group_ids, names, empty = \
                                extract_lists_from_pf(tasks[task]['workers']['groups']['group'], 'id', 'name')
                            workers_ids += group_ids
                            workers_names += names
                    if tasks[task]['workers'].get('users', None):
                        if tasks[task]['workers']['users'].get('user', None):
                            user_ids, names, empty = \
                                extract_lists_from_pf(tasks[task]['workers']['users']['user'], 'id', 'name')
                            workers_ids += user_ids
                            workers_names += names
                lists.append((
                    i201,
                    201,
                    None,
                    None,
                    json.dumps([tasks[task]['title'], tasks[task]['owner']['name'], workers_names], ensure_ascii=False),
                    json.dumps([tasks[task]['title'], tasks[task]['owner']['id'], workers_ids], ensure_ascii=False),
                ))
            if template_id == '18120272':       # Справочник подразделений - 202
                i202 += 1
                workers_ids = []
                workers_names = []
                # Добываем поле "Региональное отделение"
                ro = ''
                field_ids = []
                if tasks[task].get('customData', None):
                    if tasks[task]['customData'].get('customValue', None):
                        field_ids, empty1, empty2 = \
                            extract_lists_from_pf(tasks[task]['customData']['customValue'],'id',level_name='field')
                for i, field_id in enumerate(field_ids):
                    if field_id == '107774':
                        fields, empty1, empty2 = \
                            extract_lists_from_pf(tasks[task]['customData']['customValue'], 'text')
                        if fields[i]:
                            ro = fields[i]
                if tasks[task].get('workers', None):
                    if tasks[task]['workers'].get('groups', None):
                        if tasks[task]['workers']['groups'].get('group', None):
                            group_ids, names, empty = \
                                extract_lists_from_pf(tasks[task]['workers']['groups']['group'], 'id', 'name')
                            workers_ids += group_ids
                            workers_names += names
                    if tasks[task]['workers'].get('users', None):
                        if tasks[task]['workers']['users'].get('user', None):
                            user_ids, names, empty = \
                                extract_lists_from_pf(tasks[task]['workers']['users']['user'], 'id', 'name')
                            workers_ids += user_ids
                            workers_names += names
                lists.append((
                    i202,
                    202,
                    None,
                    None,
                    json.dumps([tasks[task]['title'], tasks[task]['owner']['name'], workers_names, ro], ensure_ascii=False),
                    json.dumps([tasks[task]['title'], tasks[task]['owner']['id'], workers_ids, ro], ensure_ascii=False),
                ))
            if template_id == '18184228':       # Воронки ПФ - 203
                i203 += 1
                # Добываем поля "Группа", "Название" и "Планировщик"
                v_group = ''
                #v_name = ''
                v_planner = ''
                field_ids = []
                if tasks[task].get('customData', None):
                    if tasks[task]['customData'].get('customValue', None):
                        field_ids, empty1, empty2 = \
                            extract_lists_from_pf(tasks[task]['customData']['customValue'],'id',level_name='field')
                for i, field_id in enumerate(field_ids):
                    if field_id == '107882':
                        fields, empty1, empty2 = \
                            extract_lists_from_pf(tasks[task]['customData']['customValue'], 'text')
                        if fields[i]:
                            v_group = fields[i]
                    if field_id == '108094':
                        fields, empty1, empty2 = \
                            extract_lists_from_pf(tasks[task]['customData']['customValue'], 'text')
                        if fields[i]:
                            v_planner = fields[i]
                lists.append((
                    i203,
                    203,
                    None,
                    None,
                    json.dumps([v_group, tasks[task]['title'], v_planner], ensure_ascii=False),
                    json.dumps([v_group, tasks[task]['title'], v_planner], ensure_ascii=False),
                ))
            if template_id == '18185928':       # Справочник Офисов - 204
                i204 += 1
                workers_ids = []
                workers_names = []
                if tasks[task].get('workers', None):
                    if tasks[task]['workers'].get('groups', None):
                        if tasks[task]['workers']['groups'].get('group', None):
                            group_ids, names, empty = \
                                extract_lists_from_pf(tasks[task]['workers']['groups']['group'], 'id', 'name')
                            workers_ids += group_ids
                            workers_names += names
                    if tasks[task]['workers'].get('users', None):
                        if tasks[task]['workers']['users'].get('user', None):
                            user_ids, names, empty = \
                                extract_lists_from_pf(tasks[task]['workers']['users']['user'], 'id', 'name')
                            workers_ids += user_ids
                            workers_names += names
                lists.append((
                    i204,
                    204,
                    None,
                    None,
                    json.dumps([tasks[task]['title'], tasks[task]['owner']['name'], workers_names], ensure_ascii=False),
                    json.dumps([tasks[task]['title'], tasks[task]['owner']['id'], workers_ids], ensure_ascii=False),
                ))
        type2list_ids = {}
        type2list_texts = {}
        for list_tuple in lists:
            if type2list_ids.get(list_tuple[1]):
                type2list_ids[list_tuple[1]].append(list_tuple[0])
                type2list_texts[list_tuple[1]].append(list_tuple[4])
            else:
                type2list_ids[list_tuple[1]] = [list_tuple[0]]
                type2list_texts[list_tuple[1]] = [list_tuple[4]]
        cur.executemany('INSERT INTO lists VALUES(?, ?, ?, ?, ?, ?);', lists)
        conn.commit()
        lists = []
        tasktemplates_ids = tuple(sorted(list(tasktemplates.keys())))
        # Формируем lists для внутренних списков и псевдо-справочников из задач
        #                                                  параллельно с загрузкой значений в amounts
        absent_fields = {}
        amounts = []
        printProgressBar(0, len(tasks), prefix='Обработано:', suffix='задач', length=50)
        for k, task in enumerate(tasks):
            if task not in tasktemplates_ids:
                printProgressBar(k, len(tasks), prefix='Обработано:', suffix='задач', length=50)
                json2field_ids = []
                json2names = []
                json2values = []
                # Если есть поля то составляем список id и значений
                if tasks[task].get('customData', None):
                    if tasks[task]['customData'].get('customValue', None):
                        json2field_ids, empty1, empty2 = \
                            extract_lists_from_pf(tasks[task]['customData']['customValue'],'id',level_name='field')
                        json2names, json2values, empty2 = \
                            extract_lists_from_pf(tasks[task]['customData']['customValue'], 'text', var2_name='value')
                # Для текстовых полей конвертируем None в ''
                for i, json2name in enumerate(json2names):
                    if json2name == None:
                        json2names[i] = ''
                for i, field_id in enumerate(json2field_ids):
                    cur.execute('SELECT type_id FROM fields WHERE id = ?', (field_id,))
                    type_ids = cur.fetchone()
                    if str(type(type_ids)).replace("'", '') != '<class tuple>':
                        if absent_fields.get(field_id):
                            absent_fields[field_id].append(tasks[task]['general'] + '-' + json2names[i])
                        else:
                            absent_fields[field_id] = [tasks[task]['general'] + '-' + json2names[i]]
                        continue
                    type_id = type_ids[0]
                    # =============================  Разбор полей по типам =============================================
                    # Типы полей заданные в ПФ -------------------------------------------------------------------------
                    if type_id == 1:                    # = Строка
                        cur.execute('INSERT INTO amounts VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?);', (
                            task,
                            None,
                            None,
                            json2names[i],
                            None,
                            None,
                            None,
                            1,
                            type_id,
                            int(field_id)
                        ))
                        conn.commit()
                    elif type_id == 2:                  # = Целое
                        cur.execute('INSERT INTO amounts VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?);', (
                            task,
                            None,
                            l(json2values[i]),
                            str(l(json2values[i])),
                            None,
                            None,
                            None,
                            1,
                            type_id,
                            int(field_id)
                        ))
                        conn.commit()
                    elif type_id == 3:                  # = float
                        cur.execute('INSERT INTO amounts VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?);', (
                            task,
                            fl(json2values[i]),
                            None,
                            str(fl(json2values[i])),
                            None,
                            None,
                            None,
                            1,
                            type_id,
                            int(field_id)
                        ))
                        conn.commit()
                    elif type_id == 4:                  # = time
                        pass # Нет ни одного поля
                    elif type_id == 5:                  # = date и datetime
                        if len(json2names[i]) > 10:
                            cur.execute('INSERT INTO amounts VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?);', (
                                task,
                                None,
                                None,
                                json2names[i],
                                datetime.strptime(json2names[i],'%d-%m-%Y %H:%M'),
                                None,
                                None,
                                1,
                                type_id,
                                int(field_id)
                            ))
                            conn.commit()
                            q=0
                        elif len(json2names[i]) == 10:
                            cur.execute('INSERT INTO amounts VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?);', (
                                task,
                                None,
                                None,
                                json2names[i],
                                datetime.strptime(json2names[i],'%d-%m-%Y'),
                                None,
                                None,
                                1,
                                type_id,
                                int(field_id)
                            ))
                            conn.commit()
                        q=0
                    elif type_id in [6, 8, 9]:          # = timedelta, = Список, = Запись справочника
                        pass # Нет ни одного поля
                    elif type_id == 7:                  # = logical
                        logical_value = -1
                        text_value = ''
                        if json2names[i].lower() == 'нет':
                            logical_value = 0
                            text_value = 'False'
                        elif json2names[i].lower() == 'да':
                            logical_value = 1
                            text_value = 'True'
                        if logical_value > -1:
                            cur.execute('INSERT INTO amounts VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?);', (
                                task,
                                None,
                                logical_value,
                                text_value,
                                None,
                                None,
                                None,
                                1,
                                type_id,
                                int(field_id)
                            ))
                            conn.commit()
                        else:
                            pass  # Нет ни одного поля
                    elif type_id == 10:                 # = Контакт
                        if json2names[i]:
                            cur.execute('INSERT INTO amounts VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?);', (
                                task,
                                None,
                                int(json2values[i]),
                                json2names[i],
                                None,
                                None,
                                None,
                                1,
                                type_id,
                                int(field_id)
                            ))
                            conn.commit()
                    elif type_id == 11:                 # = Сотрудник
                        if json2names[i]:
                            cur.execute('INSERT INTO amounts VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?);', (
                                task,
                                None,
                                int(json2values[i]),
                                json2names[i],
                                None,
                                None,
                                None,
                                1,
                                type_id,
                                int(field_id)
                            ))
                            conn.commit()
                    elif type_id == 12:                 # = Контрагент
                        if json2names[i]:
                            cur.execute('INSERT INTO amounts VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?);', (
                                task,
                                None,
                                int(json2values[i]),
                                json2names[i],
                                None,
                                None,
                                None,
                                1,
                                type_id,
                                int(field_id)
                            ))
                            conn.commit()
                    elif type_id == 13:                 # = Группа, сотрудник или контакт
                        pass # Нет ни одного поля
                        not_tested = """
                        if json2names[i]:
                            cur.execute('INSERT INTO amounts VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?);', (
                                task,
                                None,
                                int(json2values[i]),
                                json2names[i],
                                None,
                                None,
                                None,
                                1,
                                type_id,
                                int(field_id)
                            ))
                            conn.commit()
                        """
                    elif type_id == 14:                 # = Список пользователей
                        pass # Нет ни одного поля
                        not_tested = """
                        if json2names[i]:
                            if str(type(json2values[i])).replace("'", '') == '<class str>':
                                values_list = []
                                names_list = []
                                for j, value in enumerate(str(json2values[i]).strip().strip().split(';')):
                                    values_list.append(int(value))
                                for j, name in enumerate(str(json2names[i]).strip().strip().split(';')):
                                    names_list.append(name)
                                cur.execute('INSERT INTO amounts VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?);', (
                                    task,
                                    None,
                                    None,
                                    json.dumps(names_list, ensure_ascii=False),
                                    None,
                                    None,
                                    json.dumps(values_list, ensure_ascii=False),
                                    1,
                                    type_id,
                                    int(field_id)
                                ))
                                conn.commit()
                        """
                    elif type_id == 15:                 # = Набор значений справочника
                        pass # Нет ни одного поля
                        not_tested = """
                        if json2names[i]:
                            if str(type(json2values[i])).replace("'", '') == '<class str>':
                                values_list = []
                                names_list = []
                                for j, value in enumerate(str(json2values[i]).strip().strip().split(';')):
                                    values_list.append(int(value))
                                for j, name in enumerate(str(json2names[i]).strip().strip().split(';')):
                                    names_list.append(name)
                                cur.execute('INSERT INTO amounts VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?);', (
                                    task,
                                    None,
                                    None,
                                    json.dumps(names_list, ensure_ascii=False),
                                    None,
                                    None,
                                    json.dumps(values_list, ensure_ascii=False),
                                    1,
                                    type_id,
                                    int(field_id)
                                ))
                                conn.commit()
                        """
                    elif type_id == 16:                 # = Задача
                        pass # Нет ни одного поля
                        not_tested = """
                        if json2names[i]:
                            cur.execute('INSERT INTO amounts VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?);', (
                                task,
                                None,
                                int(json2values[i]),
                                json2names[i],
                                None,
                                None,
                                None,
                                1,
                                type_id,
                                int(field_id)
                            ))
                            conn.commit()
                        """
                    elif type_id == 17:                 # = Набор задач
                        pass # Нет ни одного поля
                        not_tested = """
                        if json2names[i]:
                            if str(type(json2values[i])).replace("'", '') == '<class str>':
                                values_list = []
                                names_list = []
                                for j, value in enumerate(str(json2values[i]).strip().strip().split(';')):
                                    values_list.append(int(value))
                                for j, name in enumerate(str(json2names[i]).strip().strip().split(';')):
                                    names_list.append(name)
                                cur.execute('INSERT INTO amounts VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?);', (
                                    task,
                                    None,
                                    None,
                                    json.dumps(names_list, ensure_ascii=False),
                                    None,
                                    None,
                                    json.dumps(values_list, ensure_ascii=False),
                                    1,
                                    type_id,
                                    int(field_id)
                                ))
                                conn.commit()
                        """
                    elif type_id == 20:                 # = Набор значений
                        pass # Нет ни одного поля
                        not_tested = """
                        if json2names[i]:
                            if str(type(json2values[i])).replace("'", '') == '<class str>':
                                values_list = []
                                names_list = []
                                for j, name in enumerate(str(json2names[i]).strip().strip().split(';')):
                                    names_list.append(name)
                                for j, value in enumerate(str(json2values[i]).strip().strip().split(';')):
                                    values_list.append(value)
                                cur.execute('INSERT INTO amounts VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?);', (
                                    task,
                                    None,
                                    None,
                                    json.dumps(names_list, ensure_ascii=False),
                                    None,
                                    None,
                                    json.dumps(values_list, ensure_ascii=False),
                                    1,
                                    type_id,
                                    int(field_id)
                                ))
                                conn.commit()
                        """
                    elif type_id == 21:                 # = Файлы
                        if json2names[i]:
                            if str(type(json2values[i])).replace("'", '') == '<class str>':
                                values_list = []
                                names_list = []
                                for j, value in enumerate(str(json2values[i]).strip().strip().split(';')):
                                    if value and l(value):
                                        values_list.append(int(value))
                                        names_list.append(str(value))
                                cur.execute('INSERT INTO amounts VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?);', (
                                    task,
                                    None,
                                    None,
                                    json.dumps(names_list, ensure_ascii=False),
                                    None,
                                    None,
                                    json.dumps(values_list, ensure_ascii=False),
                                    1,
                                    type_id,
                                    int(field_id)
                                ))
                                conn.commit()
                    elif type_id == 22:                 # = Проект
                        pass # Нет ни одного поля
                        not_tested = """
                        if json2names[i]:
                            cur.execute('INSERT INTO amounts VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?);', (
                                task,
                                None,
                                int(json2values[i]),
                                json2names[i],
                                None,
                                None,
                                None,
                                1,
                                type_id,
                                int(field_id)
                            ))
                            conn.commit()
                        """
                    elif type_id == 23:                 # = Итоги аналитик
                        pass # Нет ни одного поля
                        not_tested = """
                        if json2names[i]:
                            cur.execute('INSERT INTO amounts VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?);', (
                                task,
                                None,
                                None,
                                json2names[i],
                                None,
                                None,
                                None,
                                1,
                                type_id,
                                int(field_id)
                            ))
                            conn.commit()
                        """
                    elif type_id == 24:                 # Вычисляемое поле
                        pass # Нет ни одного поля
                        not_tested = """
                        if json2names[i]:
                            cur.execute('INSERT INTO amounts VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?);', (
                                task,
                                None,
                                None,
                                json2names[i],
                                None,
                                None,
                                None,
                                1,
                                type_id,
                                int(field_id)
                            ))
                            conn.commit()
                        """
                    elif type_id == 25:                 # = Местоположение
                        pass # Нет ни одного поля
                        not_tested = """
                        if json2names[i]:
                            cur.execute('INSERT INTO amounts VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?);', (
                                task,
                                None,
                                None,
                                json2names[i],
                                None,
                                None,
                                None,
                                1,
                                type_id,
                                int(field_id)
                            ))
                            conn.commit()
                        """
                    elif type_id == 26:                 # = Сумма подзадач
                        pass # Нет ни одного поля
                        not_tested = """
                        if json2names[i]:
                            cur.execute('INSERT INTO amounts VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?);', (
                                task,
                                None,
                                None,
                                json2names[i],
                                None,
                                None,
                                None,
                                1,
                                type_id,
                                int(field_id)
                            ))
                            conn.commit()
                        """
                    elif type_id == 27:                 # = Результат обучения
                        pass # Нет ни одного поля
                        not_tested = """
                        if json2names[i]:
                            cur.execute('INSERT INTO amounts VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?);', (
                                task,
                                None,
                                None,
                                json2names[i],
                                None,
                                None,
                                None,
                                1,
                                type_id,
                                int(field_id)
                            ))
                            conn.commit()
                        """
                    # Виртуальные типы (созданные для преобразоваония NoSQL в SQL) -------------------------------------
                    elif type_id in range(100, 200):    # = Списки заданные внутри ПФ
                        cur.execute('SELECT text_value FROM lists WHERE type_id = ?', (type_id,))
                        db2names = list(map(lambda x: x[0], cur.fetchall()))
                        if json2names[i] in db2names:
                            # Вариант уже есть в списке в БД - находим его id
                            cur.execute('SELECT list_id FROM lists WHERE type_id = ?', (type_id,))
                            db2list_ids = list(map(lambda x: x[0], cur.fetchall()))
                            for j, name in enumerate(db2names):
                                if name == json2names[i]:
                                    cur.execute('INSERT INTO amounts VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?);', (
                                        task,
                                        None,
                                        None,
                                        name,
                                        None,
                                        None,
                                        None,
                                        j + 1,
                                        type_id,
                                        int(field_id)
                                    ))
                                    conn.commit()
                                    break
                        else:
                            if len(db2names):
                                # Что-то есть в списке в БД - id на 1 больше максимума
                                cur.execute('SELECT list_id FROM lists WHERE type_id = ?', (type_id,))
                                db2list_ids = list(map(lambda x: x[0], cur.fetchall()))
                                list_id = max(db2list_ids) + 1
                            else:
                                # Совсем ничего нет - id = 1
                                list_id = 1
                            cur.execute('INSERT INTO lists VALUES(?, ?, ?, ?, ?, ?);', (
                                list_id,
                                type_id,
                                None,
                                None,
                                json2names[i],
                                None
                            ))
                            conn.commit()
                            cur.execute('INSERT INTO amounts VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?);', (
                                task,
                                None,
                                None,
                                json2names[i],
                                None,
                                None,
                                None,
                                list_id,
                                type_id,
                                int(field_id)
                            ))
                            conn.commit()
                        q=0
                    elif type_id in range(200, 300):    # Списки (псевдо-справочники) из задач, перевод в выделенные модели
                        if json2names[i]:
                            cur.execute('SELECT column_ids FROM types WHERE id = ?', (type_id,))
                            db2json_id = -1
                            db2json_ids = list(map(lambda x: x[0], cur.fetchall()))
                            if len(db2json_ids):
                                if db2json_ids[0]:
                                    db2json_id = int(db2json_ids[0])
                            if db2json_id > -1:
                                cur.execute('SELECT text_value FROM lists WHERE type_id = ?', (type_id,))
                                list_fetchall = cur.fetchall()
                                db2names = list(map(lambda x: json.loads(x[0])[db2json_id], list_fetchall))
                                db2names_lower = list(map(
                                    lambda x: json.loads(x[0])[db2json_id].lower().replace(' ','').replace('-',''),
                                    list_fetchall))
                                if json2names[i].lower().replace(' ','').replace('-','') in db2names_lower:
                                    # Вариант уже есть в списке в БД - находим его id
                                    for j, name in enumerate(db2names_lower):
                                        if name == json2names[i].lower().replace(' ','').replace('-',''):
                                            cur.execute('INSERT INTO amounts VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?);', (
                                                task,
                                                None,
                                                None,
                                                db2names[j],
                                                None,
                                                None,
                                                None,
                                                j + 1,
                                                type_id,
                                                int(field_id)
                                            ))
                                            conn.commit()
                                            break
                                else:
                                    # Варианта нет - создание новой записи
                                    db2name = ''
                                    if json2names[i].lower().replace(' ','').replace('-','') == 'productmanagement' \
                                            or json2names[i].lower().replace(' ','').replace('-','') == 'продблок':
                                        db2name = 'Business Development Department'
                                        db_id = 3
                                    elif json2names[i].lower().replace(' ','').replace('-','') \
                                            == '05.заявканаотзывправдоступаксистемам':
                                        db2name = '05. Заявка на доступ/отзыв доступа к системам (ds)'
                                        db_id = 43
                                    if db2name:
                                        for j, name in enumerate(db2names_lower):
                                            if name == json2names[i].lower():
                                                cur.execute('INSERT INTO amounts VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?);', (
                                                    task,
                                                    None,
                                                    None,
                                                    db2name,
                                                    None,
                                                    None,
                                                    None,
                                                    db_id + 1,
                                                    type_id,
                                                    int(field_id)
                                                ))
                                                conn.commit()
                                    else:
                                        q=0
                                    break
                        q=0
                    elif type_id in [1006,1018,1032,1040,1062,1064,1066,1070,1090,1092,1094,1096,1098]: # = Справочники
                        # Cправочники (id = idсправочника + 1000)
                        if type_id in [1006, 1032, 1040, 1062, 1064, 1066, 1070, 1090, 1092]:
                            # Справочники - кандидаты на перевод в простые списки
                            if json2names[i] in type2list_texts[type_id]:
                                cur.execute('INSERT INTO amounts VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?);', (
                                    task,
                                    None,
                                    None,
                                    json2names[i],
                                    None,
                                    None,
                                    None,
                                    type2list_ids[type_id][type2list_texts[type_id].index(json2names[i])],
                                    type_id,
                                    int(field_id)
                                ))
                                conn.commit()
                            else:
                                # Варианта нет в списке в БД - значение, которое уже удалили из справочника
                                if absent_fields.get(field_id):
                                    absent_fields[field_id].append(tasks[task]['general'] + '-' + json2names[i])
                                else:
                                    absent_fields[field_id] = [tasks[task]['general'] + '-' + json2names[i]]
                        else:
                            # Справочники - кандидаты на перевод в модели
                            #print(task, type_id, field_id, json2values[i])
                            if json2values[i] and l(json2values[i]):
                                if int(json2values[i]) in type2list_ids[type_id]:
                                    cur.execute('INSERT INTO amounts VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?);', (
                                        task,
                                        None,
                                        None,
                                        json2names[i],
                                        None,
                                        None,
                                        None,
                                        int(json2values[i]),
                                        type_id,
                                        int(field_id)
                                    ))
                                    conn.commit()
                                else:
                                    # Варианта нет в списке в БД - значение, которое уже удалили из справочника
                                    if absent_fields.get(field_id):
                                        absent_fields[field_id].append(tasks[task]['general'] + '-' + json2names[i])
                                    else:
                                        absent_fields[field_id] = [tasks[task]['general'] + '-' + json2names[i]]
                            else:
                                if absent_fields.get(field_id):
                                    absent_fields[field_id].append(tasks[task]['general'] + '-' + json2names[i])
                                else:
                                    absent_fields[field_id] = [tasks[task]['general'] + '-' + json2names[i]]
                    elif type_id in [1002, 1014, 1022, 1034, 1038, 1084, 1086, 1088]:
                        # Справочник с несколькими значениями одновременно (id = idсправочника + 1000)
                        if str(type(json2values[i])).replace("'", '') == '<class str>':
                            for j, value in enumerate(str(json2values[i]).strip().strip().split(';')):
                                if value and l(value):
                                    if int(value) in type2list_ids[type_id]:
                                        if not j:
                                            cur.execute('INSERT INTO amounts VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?);', (
                                                task,
                                                None,
                                                None,
                                                json2names[i],
                                                None,
                                                None,
                                                None,
                                                int(value),
                                                type_id,
                                                int(field_id)
                                            ))
                                            conn.commit()
                                        cur.execute('INSERT INTO arrays VALUES(?, ?, ?, ?);', (
                                            task,
                                            int(field_id),
                                            int(value),
                                            type_id
                                        ))
                                        conn.commit()
                                    else:
                                        # Варианта нет в списке в БД - значение, которое уже удалили из справочника
                                        if absent_fields.get(field_id):
                                            absent_fields[field_id].append(tasks[task]['general'] + '-' + json2names[i])
                                        else:
                                            absent_fields[field_id] = [tasks[task]['general'] + '-' + json2names[i]]
                    else:
                        if absent_fields.get(field_id):
                            absent_fields[field_id].append(int(tasks[task]['general']))
                        else:
                            absent_fields[field_id] = [int(tasks[task]['general'])]
        for absent_field in absent_fields:
            print('=========================================\nполе', absent_field,'\n', absent_fields[absent_field])















