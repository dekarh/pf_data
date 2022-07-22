# Получаем из АПИ юзеров, группы, задачи, комментарии, список файлов и сопутствующую информацию,
# скачиваем файлы из хранилища на диск, отмечаем путь/название файла в json


import json
import os
import requests
import xmltodict
from  sys import argv
import shutil
import sqlite3
import zipfile
from datetime import datetime, timedelta

from hide_data import USR_Tocken, PSR_Tocken, PF_ACCOUNT, PF_DOMAIN, TYPES, FIELDS, LISTS
from lib import l, fl

URL = "https://apiru.planfix.ru/xml"
PF_BACKUP_DIRECTORY = 'current'
PF_HEADER = {"Accept": 'application/xml', "Content-Type": 'application/xml'}
RELOAD_ALL_FROM_API = True
NOT_CHECKED_TASK = 18138396 # 18095594 # (24466 17.08.2020) До этой задачи, файлы не будут проверяться
MIN_TASK = 18243254  # (93777 от 02.02.2022) До этой задачи, задачи без файлов в дальнейшем не будут проверяться
DIR4FILES = '/opt/PF_backup/files'
DIR4JSONS = '/opt/PF_backup/data'
MAX_REPIT = 10
DOWNLOAD_FILES = True
CREATE_DB = True

def api_load_from_list(api_method, obj_name, file_name, api_additionally='',
                       pagination=True, res_dict=None, with_totalcount=True, key_name='id'):
    """
    api_method - название загружаемого метода, напр. task.getList
    obj_name - название типа загружаемого объекта в АПИ
    file_name - имя сохраняемого файла
    api_additionally - дополнительные ключи АПИ напр. <target>all</target>
    pagination - есть деление на страницы
    res_dict - словарь с ранее загруженной информацией
    with_totalcount - есть/нет @TotalCount
    key_name - имя идентификатора (id или key)
    """
    global limit_overflow
    global request_count
    if limit_overflow:
        return {}
    if res_dict is None:
        res_dict = {}
    obj_names = ''
    if obj_name[-1] == 's':
        obj_names = obj_name + 'es'
    else:
        obj_names = obj_name + 's'
    i = 1
    obj_total_count = 1000
    obj_count = 0
    if len(argv) == 1 and with_totalcount and file_name:
        printProgressBar(obj_count, obj_total_count + 1, prefix='Скачано ' + api_method + ':', suffix=obj_name,
                         length=50)
        boost = '\n'
    else:
        boost = ''
    continuation = True
    has_pages = True
    answertext = ''
    try:
        while continuation:
            i_err = 0
            while True:
                answertext = ''
                if i_err > 10:
                    if not pagination:
                        continuation = False
                    elif not has_pages:
                        continuation = False
                    break
                objs_loaded = []
                request_count += 1
                try:
                    if pagination:
                        answer = requests.post(
                            URL,
                            headers=PF_HEADER,
                            data='<request method="' + api_method + '"><account>' + PF_ACCOUNT
                                 + '</account>' + api_additionally + '<pageSize>100</pageSize><pageCurrent>'
                                 + str(i) + '</pageCurrent></request>',
                            auth=(USR_Tocken, PSR_Tocken)
                        )
                    else:
                        answer = requests.post(
                            URL,
                            headers=PF_HEADER,
                            data='<request method="' + api_method + '"><account>' + PF_ACCOUNT
                                 + '</account>' + api_additionally + '</request>',
                            auth=(USR_Tocken, PSR_Tocken)
                        )
                    answertext = answer.text
                    if not answer.ok:
                        i_err += 1
                        continue
                    elif answer.text.find('count="0"/></response>') > -1:
                        continuation = False
                        break
                    elif xmltodict.parse(answer.text)['response']['@status'] == 'error':
                        has_pages = False
                        print('\nСБОЙ №', i_err, 'в', api_method, '\nпараметры:', api_additionally, '\n', answer.text)
                        if xmltodict.parse(answer.text)['response']['code'] == '0007':
                            continuation = False
                            limit_overflow = True
                            break
                        elif xmltodict.parse(answer.text)['response']['code'] == '0015':
                            continuation = False
                            break
                        else:
                            i_err += 1
                            continue
                    else:
                        if str(type(xmltodict.parse(answer.text)['response'][obj_names])).replace("'", '') \
                                == '<class NoneType>':
                            continuation = False
                            break
                        elif str(type(xmltodict.parse(answer.text)['response'][obj_names][obj_name])).replace("'", '') \
                                == '<class NoneType>':
                            i_err += 1
                            continue
                        elif str(type(xmltodict.parse(answer.text)['response'][obj_names][obj_name])).replace("'", '') \
                                == '<class list>':
                            objs_loaded = xmltodict.parse(answer.text)['response'][obj_names][obj_name]
                            obj_count += len(objs_loaded)
                            objs_str = []
                            debug1 = """
                            for obj_loaded in objs_loaded:
                                objs_str.append(str(obj_loaded.get('id', 'нет')))
                            print('\n       ', ' '.join(objs_str))
                            """
                        else:
                            objs_loaded = [xmltodict.parse(answer.text)['response'][obj_names][obj_name]]
                            debug2 = """
                            print('\n       ', objs_loaded[0].get('id', 'нет'))
                            """
                            obj_count += 1
                        if with_totalcount:
                            if obj_total_count == 1000:
                                obj_total_count = int(
                                    xmltodict.parse(answer.text)['response'][obj_names]['@totalCount'])
                        for obj in objs_loaded:
                            res_dict[int(obj[key_name])] = obj
                        if not pagination:
                            continuation = False
                        break
                except Exception as e:
                    print('\n', datetime.now().strftime('%d.%m.%Y %H:%M:%S'), 'ОШИБКА:', e, 'в', api_method,
                          '\nпараметры:', api_additionally, '\n', answertext)
                    if not pagination:
                        continuation = False
                    break
            if len(argv) == 1 and with_totalcount and file_name:
                printProgressBar(obj_count, obj_total_count + 1, prefix='Скачано ' + api_method + ':', suffix=obj_name,
                                 length=50)
            i += 1
    finally:
        if file_name:
            with open(os.path.join(
                    PF_BACKUP_DIRECTORY,
                    list(map(lambda x:'part-' if x else '', [limit_overflow]))[0] + file_name
            ), 'w') as write_file:
                json.dump(res_dict, write_file, ensure_ascii=False)
                print(boost, datetime.now().strftime('%d.%m.%Y %H:%M:%S'), '|',request_count, '|',
                      list(map(lambda x: 'ЧАСТИЧНО' if x else '',[limit_overflow]))[0], 'Сохранено ', len(res_dict),
                      'объектов', obj_name, 'запрошенных методом', api_method)
    return res_dict


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


def reload_all():
    global request_count
    global limit_overflow
    global files
    # =============== СПРАВОЧНИКИ ==========================
    print('\n', datetime.now().strftime('%d.%m.%Y %H:%M:%S'), '|',request_count, '|',
          'Получаем из АПИ список справочников')
    handbooks = {}
    handbooks_loaded = api_load_from_list('handbook.getList', 'handbook', '', pagination=False)
    handbooks_with_category = set()
    if not limit_overflow:
        if len(argv) == 1:
            printProgressBar(0, len(handbooks_loaded) + 1, prefix='Скачаны все записи по:', suffix='справочников',
                             length=50)
        for i, handbook in enumerate(handbooks_loaded):
            records1lvl = api_load_from_list(
                'handbook.getRecords',
                'record',
                '',
                api_additionally='<handbook><id>' + str(handbook) + '</id></handbook>',
                with_totalcount=False,
                key_name='key')
            handbooks[handbook] = {}
            records2lvl = {}
            records3lvl = {}
            # Первый уровень записей справочника
            for record in records1lvl:
                if records1lvl[record]['isGroup'] == '1':
                    # Если группа, то загружаем ее для подгрузки на втором уровне
                    handbooks_with_category.add(handbook)
                    records_loaded = api_load_from_list(
                        'handbook.getRecords',
                        'record',
                        '',
                        api_additionally='<handbook><id>' + str(handbook) + '</id></handbook><parentKey>' + str(record)
                                         + '</parentKey>',
                        with_totalcount=False,
                        key_name='key')
                    for record2lvl in records_loaded:
                        records_loaded[record2lvl]['parentName'] = records1lvl[record]['name']
                    for record2lvl in records_loaded:
                        records2lvl[record2lvl] = records_loaded[record2lvl]
                else:
                    if records1lvl[record].get('customData', None):
                        if records1lvl[record]['customData'].get('customValue', None):
                            handbooks[handbook][int(record)] = {}
                            if str(type(records1lvl[record]['customData']['customValue'])).replace("'", '')\
                                    == '<class list>':
                                for field in records1lvl[record]['customData']['customValue']:
                                    handbooks[handbook][int(record)][int(field['field']['id'])] = {
                                        'text': field['text'],
                                        'value': field['value']
                                    }
                            else:
                                handbooks[handbook][int(record)][int(records1lvl[record]['customData']['customValue']['field']['id'])] = {
                                    'text': records1lvl[record]['customData']['customValue']['text'],
                                    'value': records1lvl[record]['customData']['customValue']['value']
                                }
            # Второй уровень записей справочника
            for record in records2lvl:
                if records2lvl[record]['isGroup'] == '1':
                    # Если группа, то загружаем ее для подгрузки на третьем уровне
                    records_loaded = api_load_from_list(
                        'handbook.getRecords',
                        'record',
                        '',
                        api_additionally='<handbook><id>' + str(handbook) + '</id></handbook><parentKey>' + str(record)
                                         + '</parentKey>',
                        with_totalcount=False,
                        key_name='key')
                    for record3lvl in records_loaded:
                        records_loaded[record3lvl]['parentName'] = \
                            records2lvl[int(records_loaded[record3lvl]['parentKey'])]['parentName'] \
                            + '/' + records2lvl[record]['name']
                    for record3lvl in records_loaded:
                        records3lvl[record3lvl] = records_loaded[record3lvl]
                else:
                    if records2lvl[record].get('customData', None):
                        if records2lvl[record]['customData'].get('customValue', None):
                            handbooks[handbook][int(record)] = {
                                0: {
                                    'value': records2lvl[record]['parentKey'],
                                    'text': records2lvl[record]['parentName']
                                    }
                            }
                            if str(type(records2lvl[record]['customData']['customValue'])).replace("'", '')\
                                    == '<class list>':
                                for field in records2lvl[record]['customData']['customValue']:
                                    handbooks[handbook][int(record)][int(field['field']['id'])] = {
                                        'text': field['text'],
                                        'value': field['value']
                                    }
                            else:
                                handbooks[handbook][int(record)][int(records2lvl[record]['customData']['customValue']['field']['id'])] = {
                                    'text': records2lvl[record]['customData']['customValue']['text'],
                                    'value': records2lvl[record]['customData']['customValue']['value']
                                }
            # Третий уровень записей справочника
            for record in records3lvl:
                # Если группа, то четвертый уровень не поддерживается
                if records3lvl[record]['isGroup'] == '1':
                    print('4 уровень не поддерживается!!!', handbook, record)
                else:
                    if records3lvl[record].get('customData', None):
                        if records3lvl[record]['customData'].get('customValue', None):
                            handbooks[handbook][int(record)] = {
                                0: {
                                    'value': records3lvl[record]['parentKey'],
                                    'text': records3lvl[record]['parentName']
                                    }
                            }
                            if str(type(records3lvl[record]['customData']['customValue'])).replace("'", '')\
                                    == '<class list>':
                                for field in records3lvl[record]['customData']['customValue']:
                                    handbooks[handbook][int(record)][int(field['field']['id'])] = {
                                        'text': field['text'],
                                        'value': field['value']
                                    }
                            else:
                                handbooks[handbook][int(record)][int(records3lvl[record]['customData']['customValue']['field']['id'])] = {
                                    'text': records3lvl[record]['customData']['customValue']['text'],
                                    'value': records3lvl[record]['customData']['customValue']['value']
                                }

            if len(argv) == 1:
                printProgressBar(i, len(handbooks) + 1, prefix='Скачаны все записи по:', suffix='справочников',
                                 length=50)
        for handbook in handbooks_with_category:
            for record in handbooks[handbook]:
                if not handbooks[handbook][record].get(0, False):
                    handbooks[handbook][record][0] = {
                        'text': 'б/к',
                        'value': 'б/к'
                    }

        with open(os.path.join(PF_BACKUP_DIRECTORY, 'handbooks_full.json'), 'w') as write_file:
            json.dump(handbooks, write_file, ensure_ascii=False)

    # =============== ЗАДАЧИ ==========================
    print('\n', datetime.now().strftime('%d.%m.%Y %H:%M:%S'), '|',request_count, '|',
          'Загружаем бэкап задач (task.getMulti скорректированной task.get) через АПИ ПФ')
    tasks_full = {}
    tasks_short = {}
    all_tasks_ids = set()
    with open(os.path.join(PF_BACKUP_DIRECTORY, 'tasks_full.json'), 'r') as read_file:
        tasks_full_str = json.load(read_file)
    for task in tasks_full_str:
        all_tasks_ids.add(int(task))
        tasks_full[int(task)] = tasks_full_str[task]
    all_tasks_ids_tuple =  tuple(sorted(all_tasks_ids))
    print('\n',datetime.now().strftime('%d.%m.%Y %H:%M:%S'), '|',request_count, '|',
          'Из сохраненных полных (task.getMulti):', len(tasks_full), 'Всего везде:', len(all_tasks_ids_tuple))

    print('\n', datetime.now().strftime('%d.%m.%Y %H:%M:%S'), '|',request_count, '|',
          'Получаем из АПИ задачи из task.getList')
    if not limit_overflow:
        tasks_short = api_load_from_list('task.getList', 'task', 'tasks_short.json',
                                         api_additionally='<target>all</target>')
        for task in tasks_short:
            all_tasks_ids.add(int(task))
        all_tasks_ids_tuple =  tuple(sorted(all_tasks_ids))

    print('\n', datetime.now().strftime('%d.%m.%Y %H:%M:%S'), '|',request_count, '|',
          'Догружаем найденные задачи в полный бэкап tasks_full. Всего везде:', len(all_tasks_ids_tuple))
    if not limit_overflow:
        not_finded_tasks_ids = set()
        tasks4check = set()
        deleted_tasks_ids = set()
        hundred4xml = []
        hundred_ids = []
        tasks_count = len(all_tasks_ids)
        tasks_full_checked = {}
        for task in tasks_full:
            if not tasks_short.get(task, None):
                if tasks_full[task]['type'] == 'task':
                    tasks4check.add(task)
        tasks4check = tuple(sorted(list(tasks4check)))
        if len(argv) == 1:
            printProgressBar(0, tasks_count + 1, prefix='Скачано полных:', suffix='задач', length=50)
        try:
            for task in all_tasks_ids_tuple:
                if not tasks_full.get(task, None) or task in tasks4check:
                    hundred_ids += [int(task)]
                    hundred4xml += ['<id>' + str(task) + '</id>']
                    if len(hundred_ids) > 99:
                        i = 0
                        while True:
                            tasks_loaded = []
                            try:
                                if i > 10:
                                    for hundred_id in hundred_ids:
                                        not_finded_tasks_ids.add(hundred_id)
                                    break
                                answer = requests.post(
                                    URL,
                                    headers=PF_HEADER,
                                    data='<request method="task.getMulti"><account>' + PF_ACCOUNT +
                                         '</account><tasks>' + ''.join(hundred4xml) + '</tasks></request>',
                                    auth=(USR_Tocken, PSR_Tocken)
                                )
                                if not answer.ok:
                                    i += 1
                                    continue
                                elif xmltodict.parse(answer.text)['response']['@status'] != 'ok':
                                    i += 1
                                    continue
                                elif answer.text.find('count="0"/></response>') > -1:
                                    for hundred_id in hundred_ids:
                                        not_finded_tasks_ids.add(hundred_id)
                                    break
                                elif not len(xmltodict.parse(answer.text)['response']['tasks']['task']):
                                    i += 1
                                    continue
                                else:
                                    if str(type(xmltodict.parse(answer.text)['response']['tasks']['task'])).replace("'", '') == '<class list>':
                                        tasks_loaded = xmltodict.parse(answer.text)['response']['tasks']['task']
                                    elif str(type(xmltodict.parse(answer.text)['response']['tasks']['task'])).replace("'", '') == '<class NoneType>':
                                        i += 1
                                        continue
                                    else:
                                        tasks_loaded = [xmltodict.parse(answer.text)['response']['tasks']['task']]
                                    for ids in hundred_ids:
                                        finded_ids = False
                                        for task_loaded in tasks_loaded:
                                            if int(task_loaded['id']) == ids:
                                                finded_ids = True
                                                tasks_full[ids] = task_loaded
                                        if not finded_ids:
                                            not_finded_tasks_ids.add(ids)
                                    break
                            except Exception as e:
                                i += 1
                                continue
                        hundred4xml = []
                        hundred_ids = []
                    if len(argv) == 1:
                        printProgressBar(len(tasks_full), tasks_count + 1, prefix='Скачано полных:', suffix='задач',
                                         length=50)
                if os.path.exists(os.path.join(PF_BACKUP_DIRECTORY, 'tasks_full_stop')):
                    raise ValueError
        finally:
            print('\n', datetime.now().strftime('%d.%m.%Y %H:%M:%S'), '|',request_count, '|', 'Всего везде:',
                  len(all_tasks_ids_tuple), 'Сохранено:', len(tasks_full), 'Не найдено:', len(not_finded_tasks_ids))
            for task in tasks_full:             # Обновляем во всех задачах информацию из tasks_short_dict
                if tasks_short.get(task, None):
                    for task_property in tasks_short[task]:
                        tasks_full[task][task_property] = tasks_short[task][task_property]
            for k, task in enumerate(not_finded_tasks_ids):
                i = 0
                while True:
                    try:
                        if i > 10:
                            deleted_tasks_ids.add(task)
                            break
                        answer = requests.post(
                            URL,
                            headers=PF_HEADER,
                            data='<request method="task.get"><account>' + PF_ACCOUNT +
                                 '</account><task><id>' + str(task) + '</id></task></request>',
                            auth=(USR_Tocken, PSR_Tocken)
                        )
                        if not answer.ok:
                            i += 1
                            continue
                        elif xmltodict.parse(answer.text)['response']['@status'] == 'error' \
                                and xmltodict.parse(answer.text)['response']['code'] == '3001':
                            deleted_tasks_ids.add(task)
                            break
                        else:
                            if str(type(xmltodict.parse(answer.text)['response']['task'])).replace("'", '') \
                                    == '<class NoneType>':
                                i += 1
                                continue
                            else:
                                tasks_full[task] = xmltodict.parse(answer.text)['response']['task']
                            break
                    except Exception as e:
                        i += 1
                        continue
                if len(argv) == 1:
                    printProgressBar(k, len(not_finded_tasks_ids) + 1, prefix='Скачано:', suffix='задач', length=50)
            # Удаляем неподтверждённые задачи
            for task in tasks_full:
                if task not in deleted_tasks_ids:
                    tasks_full_checked[task] = tasks_full[task]
            print(datetime.now().strftime('%d.%m.%Y %H:%M:%S'), '|',request_count, '|', 'Удалено:',
                  len(deleted_tasks_ids), 'осталось:', len(tasks_full_checked))
            with open(os.path.join(PF_BACKUP_DIRECTORY, 'tasks_full.json'), 'w') as write_file:
                    json.dump(tasks_full_checked, write_file, ensure_ascii=False)

    all_tasks_ids = set()
    for task in tasks_full_checked:
        all_tasks_ids.add(int(task))
    all_tasks_ids_tuple = tuple(sorted(all_tasks_ids))

    # =============== ФАЙЛЫ + ПРОЕКТЫ + КОНТАКТЫ ==========================
    print('\n', datetime.now().strftime('%d.%m.%Y %H:%M:%S'), '|',request_count, '|',
          'Загружаем ранее полученный из АПИ список файлов')
    task_numbers_from_loaded_files = set()
    with open(os.path.join(PF_BACKUP_DIRECTORY, 'files_full.json'), 'r') as read_file:
        files_loaded = json.load(read_file)
    for file in files_loaded:
        files[int(file)] = files_loaded[file]
        if files[int(file)].get('task', None):
            if files[int(file)]['task'].get('id', None):
                task_numbers_from_loaded_files.add(int(files[int(file)]['task']['id']))
    task_numbers_from_loaded_files = tuple(task_numbers_from_loaded_files)

    task_without_files = []
    tasks4check = []
    #task_without_files_general = []
    for task in all_tasks_ids_tuple:
        if task < NOT_CHECKED_TASK:
            task_without_files.append(task)
        elif task < MIN_TASK and task not in task_numbers_from_loaded_files:
            task_without_files.append(task)
            #task_without_files_general.append(int(tasks_full[task]['general']))
        else:
            tasks4check.append(task)
    task_without_files = tuple(task_without_files)
    #task_without_files_general = tuple(task_without_files_general)
    tasks4check = sorted(tasks4check)
    print('\n', datetime.now().strftime('%d.%m.%Y %H:%M:%S'), '|',request_count, '|', 'Из', len(tasks_full), 'задач',
          len(tasks4check), 'будут, а', len(task_without_files), 'не будут проверяться')

    print('\n', datetime.now().strftime('%d.%m.%Y %H:%M:%S'), '|',request_count, '|',
          'Получаем из АПИ файлы по каждой задаче')
    if not limit_overflow:
        if len(argv) == 1:
            printProgressBar(0, len(tasks4check) + 1, prefix='Скачаны все файлы по:', suffix='задач', length=50)
        for i, task in enumerate(tasks4check):
            addition_text = '<task><id>' + str(task) + '</id></task>' \
                            + '<returnDownloadLinks>1</returnDownloadLinks>'
            files_loaded = api_load_from_list('file.getListForTask', 'file', '',
                                              api_additionally=addition_text)
            for file in files_loaded:
                if not files.get(file, None):
                    files[file] = files_loaded[file]
            if len(argv) == 1:
                printProgressBar(i, len(tasks4check) + 1, prefix='Скачаны все файлы по:', suffix='задач', length=50)
            else:
                print(datetime.now().strftime('%d.%m.%Y %H:%M:%S'), 'Задача №', tasks_full[task].get('general', 'б/н'),
                      '[', task, ']  (', i, 'из', len(tasks4check), ')')

    print('\n', datetime.now().strftime('%d.%m.%Y %H:%M:%S'), '|',request_count, '|',
          'Получаем из АПИ дерево проектов (переименовал внутри flectra в hr.projectgroup)')
    projectgroups = api_load_from_list('project.getList', 'project', 'projectgroups_full.json')

    print('\n', datetime.now().strftime('%d.%m.%Y %H:%M:%S'), '|',request_count, '|',
          'Получаем из АПИ список файлов по каждому проекту')
    if not limit_overflow:
        if len(argv) == 1:
            printProgressBar(0, len(projectgroups) + 1, prefix='Скачаны все файлы по:', suffix='проектов',
                             length=50)
        for i, projectgroup in enumerate(projectgroups):
            addition_text = '<project><id>' + str(projectgroup) + '</id></project>' \
                            + '<returnDownloadLinks>1</returnDownloadLinks>'
            files_loaded = api_load_from_list('file.getListForProject', 'file', '',
                                              api_additionally=addition_text)
            for file in files_loaded:
                files[file] = files_loaded[file]
            if len(argv) == 1:
                printProgressBar(i, len(projectgroups) + 1, prefix='Скачаны все файлы по:', suffix='проектов',
                                 length=50)

    print('\n', datetime.now().strftime('%d.%m.%Y %H:%M:%S'), '|',request_count, '|',
          'Получаем из АПИ контакты')
    contacts = api_load_from_list('contact.getList', 'contact', 'contacts_full.json')

    print('\n', datetime.now().strftime('%d.%m.%Y %H:%M:%S'), '|',request_count, '|',
          'Получаем из АПИ список файлов по каждому контакту')
    if not limit_overflow:
        if len(argv) == 1:
            printProgressBar(0, len(contacts) + 1, prefix='Скачаны все файлы по:', suffix='контактов', length=50)
        for i, contact in enumerate(contacts):
            addition_text = '<client><id>' + str(contact) + '</id></client>' \
                            + '<returnDownloadLinks>1</returnDownloadLinks>'
            files_loaded = api_load_from_list('file.getListForClient', 'file', '',
                                              api_additionally=addition_text)
            for file in files_loaded:
                files[file] = files_loaded[file]
                files[file]['contragent'] = contact
            if len(argv) == 1:
                printProgressBar(i, len(contacts) + 1, prefix='Скачаны все файлы по:', suffix='контактов',
                                 length=50)

    print('\n', datetime.now().strftime('%d.%m.%Y %H:%M:%S'), '|',request_count, '|',
          'Сохраняем результирующий список файлов')
    if not limit_overflow:
        with open(os.path.join(PF_BACKUP_DIRECTORY, 'files_full.json'), 'w') as write_file:
            json.dump(files, write_file, ensure_ascii=False)

    # =============== КОММЕНТАРИИ  ==========================
    print('\n', datetime.now().strftime('%d.%m.%Y %H:%M:%S'), '|',request_count, '|',
          'Загружаем бэкап комментариев')
    actions = {}
    with open(os.path.join(PF_BACKUP_DIRECTORY, 'actions_full.json'), 'r') as read_file:
        actions_str = json.load(read_file)
    for action in actions_str:
        actions[int(action)] = actions_str[action]
    print(datetime.now().strftime('%d.%m.%Y %H:%M:%S'), '|',request_count, '|', 'Из сохраненных комментариев:',
          len(actions))

    print(datetime.now().strftime('%d.%m.%Y %H:%M:%S'), '|',request_count, '|', 'Догружаем комментарии')
    addition_text = '<fromDate>' \
                    + (datetime.strptime(actions[max(actions.keys())]['dateTime'], '%d-%m-%Y %H:%M') -
                       timedelta(minutes=1)).strftime('%d-%m-%Y %H:%M') \
                    + '</fromDate><toDate>' \
                    + datetime.now().strftime('%d-%m-%Y %H:%M') \
                    + '</toDate><sort>asc</sort>'
    api_load_from_list('action.getListByPeriod', 'action', 'actions_full.json',
                       api_additionally=addition_text, res_dict=actions)

    # =============== юзеры, сотрудники, контакты, группы доступа, шаблоны задач, процессы, статусы ===================
    print('\n', datetime.now().strftime('%d.%m.%Y %H:%M:%S'), '|',request_count, '|',
          'Получаем из АПИ юзеров, сотрудников, контакты, группы доступа и шаблоны задач')
    api_load_from_list('user.getList', 'user', 'users_full.json')
    api_load_from_list('contact.getList', 'contact', 'contacts_' + PF_ACCOUNT + '.json' ,
                       api_additionally='<target>6532326</target>')
    api_load_from_list('userGroup.getList', 'userGroup', 'usergroups_full.json')
    api_load_from_list('task.getList', 'task', 'tasktemplates_full.json',
                       api_additionally='<target>template</target>')

    print('\n', datetime.now().strftime('%d.%m.%Y %H:%M:%S'), '|',request_count, '|',
          'Получаем из АПИ список процессов')
    processes = api_load_from_list('taskStatus.getSetList', 'taskStatusSet', 'processes_full.json',
                                   pagination=False)

    print('\n', datetime.now().strftime('%d.%m.%Y %H:%M:%S'), '|',request_count, '|',
          'Получаем из АПИ список статусов по каждому процессу')
    statuses = {}
    # inactive = set()
    if not limit_overflow:
        if len(argv) == 1:
            printProgressBar(0, len(processes) + 1, prefix='Скачаны все статусы по:', suffix='процессов', length=50)
        for i, process in enumerate(processes):
            addition_text = '<taskStatusSet><id>' + str(process) + '</id></taskStatusSet>'
            statuses_loaded = api_load_from_list('taskStatus.getListOfSet', 'taskStatus', '',
                                          api_additionally=addition_text, pagination=False)
            if len(argv) == 1:
                printProgressBar(i, len(processes) + 1, prefix='Скачаны все статусы по:', suffix='процессов', length=50)
            for status in statuses_loaded:
                #if statuses_loaded[status]['isActive'] == '0':
                #    inactive.add(int(status))
                statuses['st_' + str(status) + processes[process]['name'].split('(')[1].split(')')[0]] = {
                    'name': statuses_loaded[status]['name'],
                    'id_pf': str(status),
                    'project_ids': ['pr_' + str(process)],
                }
        with open(os.path.join(PF_BACKUP_DIRECTORY, 'statuses_flectra.json'), 'w') as write_file:
            json.dump(statuses, write_file, ensure_ascii=False)
        #inactive.add(211)   # Шаблон сформирован
        #inactive.add(210)   # Договор заключен
        #inactive.add(5)  # Отклоненная
        #inactive.add(249) # НЕ согласовано
        #inactive.add(143)  # Заявка исполнена
        #inactive.add(147)  # Заявка отменена/отклонена
        #inactive.remove(4)  # Отложенная
        #with open(os.path.join(PF_BACKUP_DIRECTORY, 'inactive_statuses.json'), 'w') as write_file:
        #    json.dump(list(inactive), write_file, ensure_ascii=False)
    print('\nВСЕГО потрачено запросов:', request_count)


def check_parent_id(tid, tdict):
    if tid:
        if tdict.get(int(tdict[int(tid)]['parent']['id']), None):
            return int(tdict[int(tid)]['parent']['id'])
        else:
            return None
    else:
        return None


def download_file(rez_str, downloaded_files_ids):
    if not os.path.exists(rez_str) and int(file['id']) not in downloaded_files_ids:
        downloaded = False
        repit = 0
        skipping = False
        while not downloaded:
            if not str(file['downloadLink']).startswith(PF_DOMAIN):
                answer = file['downloadLink']
                downloaded_files_ids += [int(file['id'])]
                skipping = True
                break
            try:
                answer = requests.get(file['downloadLink'])
            except Exception as e:
                repit += 1
                errors[int(file['id'])] = e
                if repit > MAX_REPIT:
                    skipping = True
                    break
                else:
                    continue
            if answer.reason == 'Not Found' or answer.reason == 'Forbidden':
                downloaded_files_ids += [int(file['id'])]
                skipping = True
                break
            if not answer.ok:
                repit += 1
                errors[int(file['id'])] = answer.reason
                if repit > MAX_REPIT:
                    downloaded_files_ids += [int(file['id'])]
                    skipping = True
                    break
            downloaded = answer.ok
        if not skipping:
            downloaded_files_ids += [int(file['id'])]
            print(datetime.now().strftime("%H:%M:%S"), i, '|', j + 1, 'из',
                  len(projects_in_levels[i]), rez_str)
            try:
                with open(rez_str, 'wb') as f:
                    f.write(answer.content)
                return rez_str, downloaded_files_ids
            except OSError as exc:
                if exc.errno in [36,2]:
                    rez_str = rez_str[:164] + '.' + rez_str.split('.')[-1]
                    print('Сократили до', len(rez_str), 'символов:\n', rez_str)
                    with open(rez_str, 'wb') as f:
                        f.write(answer.content)
                    return rez_str, downloaded_files_ids
                else:
                    raise  # re-raise previously caught exception
        else:
            with open('errors.log', 'a') as f:
                f.write('\n' + datetime.now().strftime("%H:%M:%S") +
                        '\n================================ ФАЙЛ =================================\n')
                json.dump(file, f, ensure_ascii=False)
                f.write('\n================================ ПРОЕКТ =================================\n')
                json.dump(projects[project], f, ensure_ascii=False)
                f.write('\n================================ ОТВЕТ =================================\n')
                f.write(str(answer))
                f.write('\n================================ ОШИБКА =================================\n')
                f.write(str(errors.get(int(file['id']))))
                f.write('\n--------------------------------------------------------------------------\n')
                return rez_str, downloaded_files_ids
    else:
        return rez_str, downloaded_files_ids


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

def backup2variables():
    # Загружаем шаблоны задач
    tasktemplates = {}
    with open(os.path.join(PF_BACKUP_DIRECTORY, 'tasktemplates_full.json'), 'r') as read_file:
        loaded_tasktemplates = json.load(read_file)
    for tasktemplate in loaded_tasktemplates:
        tasktemplates[int(tasktemplate)] = loaded_tasktemplates[tasktemplate]
    # Загружаем справочники
    with open(os.path.join(PF_BACKUP_DIRECTORY, 'handbooks_full.json'), 'r') as read_file:
        handsbooks_loaded = json.load(read_file)
    handsbooks = {}
    for handsbook in handsbooks_loaded:
        handsbooks[int(handsbook)] = handsbooks_loaded[handsbook]
    # Загружаем задачи
    with open(os.path.join(PF_BACKUP_DIRECTORY, 'tasks_full.json'), 'r') as read_file:
        tasks_loaded = json.load(read_file)
    tasks = {}
    for task in tasks_loaded:
        tasks[int(task)] = tasks_loaded[task]

    if CREATE_DB:
        if os.path.exists(os.path.join(PF_BACKUP_DIRECTORY, 'fields.db')):
            os.remove(os.path.join(PF_BACKUP_DIRECTORY, 'fields.db'))
    conn = sqlite3.connect(os.path.join(PF_BACKUP_DIRECTORY, 'fields.db'))
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
        if len(argv) == 1:
            printProgressBar(0, len(tasks), prefix='Обработано:', suffix='задач', length=50)
        for k, task in enumerate(tasks):
            if task not in tasktemplates_ids:
                if len(argv) == 1:
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

if __name__ == "__main__":
    # =========================================== Загружаем все из АПИ =================================================
    request_count = 0
    limit_overflow = False
    files = {}
    reload_all()
    if os.path.exists(DIR4FILES):
        # ============================== Догружаем появившиеся, удаляем неактуальные файлы  ============================
        if not limit_overflow:
            # Обнуляем пути к файлу на диске
            for file in files:
                files[file]['full_path'] = ''
            # Загружаем дерево проектов из сохраненного файла,
            # добавляем безопасные для файловой системы названия проектов
            projects = {}
            with open(os.path.join(PF_BACKUP_DIRECTORY, 'projectgroups_full.json'), 'r') as read_file:
                projects_loaded = json.load(read_file)
            for project in projects_loaded:
                projects[int(project)] = projects_loaded[project]
                projects[int(project)]['title_recycled'] = \
                    projects[int(project)]['title'].replace(':', '-').replace('\\', '-').replace('/', '-')

            tasks_full = {}
            with open(os.path.join(PF_BACKUP_DIRECTORY, 'tasks_full.json'), 'r') as read_file:
                tasks_full_loaded = json.load(read_file)
            for task in tasks_full_loaded:
                tasks_full[int(task)] = tasks_full_loaded[task]

            projects_in_levels = {0: [], 1: [], 2: [], 3: [], 4: [], 5: [], 6: [], }
            for project in projects:
                # Вычисляем уровень вложенности текущего проекта (количество уровней надпроектов)
                down_recursion = True
                project_ids = []
                current_id = projects[project]['id']
                while down_recursion:
                    parent_id = check_parent_id(current_id, projects)
                    if parent_id:
                        project_ids.append(parent_id)
                        current_id = parent_id
                    else:
                        down_recursion = False
                projects[project]['level'] = len(project_ids)
                projects[project]['parents'] = project_ids
                projects_in_levels[len(project_ids)] += [project]

            # Создаем дерево директорий

            for i, project_in_level in enumerate(projects_in_levels):
                for project in projects_in_levels[project_in_level]:
                    full_path = DIR4FILES
                    if len(projects[project]['parents']) > 1:
                        projects_parents = reversed(projects[project]['parents'])
                    else:
                        projects_parents = projects[project]['parents']
                    for parent_project in projects_parents:
                        full_path = os.path.join(full_path, projects[parent_project]['title_recycled'])
                    full_path = os.path.join(full_path, projects[project]['title_recycled'])
                    projects[project]['full_path'] = full_path
                    if DOWNLOAD_FILES:
                        if not os.path.exists(full_path):
                            os.mkdir(full_path)
            if DOWNLOAD_FILES:
                if not os.path.exists(os.path.join(DIR4FILES, '_Без_проекта')):
                    os.mkdir(os.path.join(DIR4FILES, '_Без_проекта'))

            downloaded_files_ids = []
            errors = {}
            try:
                for i in range(len(projects_in_levels) - 1, -1, -1):
                    print('\n', datetime.now().strftime("%H:%M:%S"),
                          '  ======================= УРОВЕНЬ', i, '===========================\n')
                    for j, project in enumerate(projects_in_levels[i]):
                        print(datetime.now().strftime("%H:%M:%S"), 'УРОВЕНЬ', i, '|Проект', j + 1, 'из',
                              len(projects_in_levels[i]))
                        project_files = []
                        for file in files:
                            if files[file].get('project', None):
                                if files[file]['project'].get('id', None):
                                    if int(files[file]['project']['id']) == project:
                                        project_files.append(files[file])
                        rez_str = ''
                        for file in project_files:
                            answer = ''
                            if file.get('task', None):
                                if file['task'].get('id', None):
                                    if tasks_full.get(int(file['task']['id']), None):
                                        write_path = os.path.join(
                                            projects[project]['full_path'],
                                            str(tasks_full[int(file['task']['id'])]['general']))
                                    else:
                                        write_path = projects[project]['full_path']
                                else:
                                    write_path = projects[project]['full_path']
                            else:
                                write_path = projects[project]['full_path']
                            if DOWNLOAD_FILES:
                                if not os.path.exists(write_path):
                                    os.mkdir(write_path)
                            rez_str = str(os.path.join(write_path, file['name'].replace('~&gt;', '').replace('&', '').\
                                                       replace(';', '').replace(':', '').replace('~', '').replace('/', '').\
                                                       replace('\\', '')))
                            if len(rez_str) > 230:
                                rez_str = rez_str[:224] + '.' + rez_str.split('.')[-1]
                            if DOWNLOAD_FILES:
                                rez_str, downloaded_files_ids = download_file(rez_str, downloaded_files_ids)
                            files[int(file['id'])]['full_path'] = rez_str
                print('\n', datetime.now().strftime("%H:%M:%S"),
                      '  ======================= БЕЗ ПРОЕКТА ===========================\n')
                without_project = set()
                j = 0
                for file in files.values():
                    if not file.get('full_path', ''):
                        if tasks_full.get(int(file['task']['id'])):
                            rez_str = os.path.join(DIR4FILES, '_Без_проекта',
                                                   str(tasks_full[int(file['task']['id'])]['general']))
                        else:
                            rez_str = os.path.join(DIR4FILES, '_Без_проекта', file['task']['id'])
                        if not os.path.exists(rez_str):
                            os.mkdir(rez_str)
                        rez_str = os.path.join(rez_str, file['name'].replace('~&gt;', '').replace('&', '').replace(';', '')
                                               .replace(':', '').replace('~', '').replace('/', '').replace('\\', ''))
                        if len(rez_str) > 230:
                            rez_str = rez_str[:224] + '.' + rez_str.split('.')[-1]
                        if tasks_full.get(int(file['task']['id'])):
                            without_project.add(str(tasks_full[int(file['task']['id'])]['general']))
                        if DOWNLOAD_FILES:
                            rez_str, downloaded_files_ids = download_file(rez_str, downloaded_files_ids)
                        files[int(file['id'])]['full_path'] = rez_str
                print(list(without_project).sort())
            except Exception as e:
                print('ОШИБКА!!!', e)
            finally:
                pass
                with open(os.path.join(PF_BACKUP_DIRECTORY, 'files_full.json'), 'w') as write_file:
                    json.dump(files, write_file, ensure_ascii=False)

            # Сохраняем список загруженных на диск файлов
            files_from_disk = []
            for root, dirs, dir_files in os.walk(DIR4FILES):
                for file in dir_files:
                    files_from_disk.append(os.path.join(root, file))
            with open(os.path.join(PF_BACKUP_DIRECTORY, 'files_from_disk.json'), 'w') as write_file:
                json.dump(files_from_disk, write_file, ensure_ascii=False)
            files_from_disk = tuple(sorted(files_from_disk))

            # Список файлов из АПИ ПФ
            files_from_api = []
            for file in files:
                files_from_api.append(files[file]['full_path'])
            files_from_api = tuple(sorted(files_from_api))

            if len(argv) == 1:
                printProgressBar(0, len(files_from_disk), prefix='Проверено:', suffix='на удаление', length=50)
            files4delete = []
            for i, file in enumerate(files_from_disk):
                if len(argv) == 1:
                    printProgressBar(i, len(files_from_disk), prefix='Проверено:', suffix='на удаление', length=50)
                if file not in files_from_api:
                    if not os.path.exists(os.path.dirname(file.replace('/files/', '/deleted/'))):
                        os.makedirs(os.path.dirname(file.replace('/files/', '/deleted/')), exist_ok=False)
                    shutil.move(file, file.replace('/files/', '/deleted/'))

            # Обновляем список загруженных на диск файлов
            files_from_disk = []
            for root, dirs, dir_files in os.walk(DIR4FILES):
                for file in dir_files:
                    files_from_disk.append(os.path.join(root, file))
            with open(os.path.join(PF_BACKUP_DIRECTORY, 'files_from_disk.json'), 'w') as write_file:
                json.dump(files_from_disk, write_file, ensure_ascii=False)
            files_from_disk = tuple(sorted(files_from_disk))

            # ============================== Загружаем структуру и значения полей в SQLite базу ========================
            backup2variables()

            # Архивируем скачанные из АПИ JSON и БД SQLite
            z = zipfile.ZipFile(os.path.join(DIR4JSONS, datetime.now().strftime("%Y-%m-%d")) + '.zip', 'w')
            for f_root, f_dirs, f_files in os.walk(PF_BACKUP_DIRECTORY):
                for file in f_files:
                    z.write(os.path.join(f_root, file))  # Создание относительных путей и запись файлов в архив
            z.close()
        else:
            print('\n', datetime.now().strftime("%H:%M:%S"), 'Неполное обновление - обновление файлов и полей ' +
                  'ПРОПУЩЕНО')
    else:
        print('\n', datetime.now().strftime("%H:%M:%S"), 'Не смонтирован диск с файлами - обновление файлов и полей ' +
              'ПРОПУЩЕНО')

    print('\n', datetime.now().strftime("%H:%M:%S"),
          '============================ РАБОТА СКРИПТА ОКОНЧЕНА ============================', '\n\n')











