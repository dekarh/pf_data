# Получаем из АПИ юзеров, группы, задачи, комментарии, список файлов и сопутствующую информацию,
# скачиваем файлы из хранилища на диск, отмечаем путь/название файла в json


import json
import os
import requests
import xmltodict
from  sys import argv
import shutil
from datetime import datetime, timedelta

from hide_data import USR_Tocken, PSR_Tocken, PF_ACCOUNT

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
                            debug2 = """
                            objs_loaded = [xmltodict.parse(answer.text)['response'][obj_names][obj_name]]
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

    # =============== ВСЁ ОСТАЛЬНОЕ ==========================
    print('\n', datetime.now().strftime('%d.%m.%Y %H:%M:%S'), '|',request_count, '|',
          'Получаем из АПИ юзеров, сотрудников, контакты, группы доступа и шаблоны задач')
    api_load_from_list('user.getList', 'user', 'users_full.json')
    api_load_from_list('contact.getList', 'contact', 'contacts_finfort.json' ,
                       api_additionally='<target>6532326</target>')
    api_load_from_list('userGroup.getList', 'userGroup', 'usergroups_full.json')
    api_load_from_list('task.getList', 'task', 'tasktemplates_full.json',
                       api_additionally='<target>template</target>')

    print('\n', datetime.now().strftime('%d.%m.%Y %H:%M:%S'), '|',request_count, '|',
          'Получаем из АПИ список справочников')
    handbooks = api_load_from_list('handbook.getList', 'handbook', '', pagination=False)
    if not limit_overflow:
        if len(argv) == 1:
            printProgressBar(0, len(handbooks) + 1, prefix='Скачаны все записи по:', suffix='справочников', length=50)
        for i, handbook in enumerate(handbooks):
            addition_text = '<handbook><id>' + str(handbook) + '</id></handbook>'
            records = api_load_from_list('handbook.getRecords', 'record', '',
                                         api_additionally=addition_text, with_totalcount=False, key_name='key')
            handbooks[handbook]['records'] = records
            if len(argv) == 1:
                printProgressBar(i, len(handbooks) + 1, prefix='Скачаны все записи по:', suffix='справочников',
                                 length=50)
        with open(os.path.join(PF_BACKUP_DIRECTORY, 'handbooks_full.json'), 'w') as write_file:
            json.dump(handbooks, write_file, ensure_ascii=False)

    print('\n', datetime.now().strftime('%d.%m.%Y %H:%M:%S'), '|',request_count, '|',
          'Получаем из АПИ список процессов')
    processes = api_load_from_list('taskStatus.getSetList', 'taskStatusSet', 'processes_full.json',
                                   pagination=False)

    print('\n', datetime.now().strftime('%d.%m.%Y %H:%M:%S'), '|',request_count, '|',
          'Получаем из АПИ список статусов по каждому процессу')
    statuses = {}
    inactive = set()
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
                if statuses_loaded[status]['isActive'] == '0':
                    inactive.add(int(status))
                if statuses.get('st_' + str(status), None):
                    statuses['st_' + str(status)]['project_ids'] += ['pr_' + str(process)]
                else:
                    statuses['st_' + str(status)] = {
                        'name': statuses_loaded[status]['name'],
                        'id_pf': str(status),
                        'project_ids': ['pr_' + str(process)],
                }
        with open(os.path.join(PF_BACKUP_DIRECTORY, 'statuses_flectra.json'), 'w') as write_file:
            json.dump(statuses, write_file, ensure_ascii=False)
        inactive.add(211)   # Шаблон сформирован
        inactive.add(210)   # Договор заключен
        inactive.add(5)  # Отклоненная
        inactive.add(249) # НЕ согласовано
        inactive.add(143)  # Заявка исполнена
        inactive.add(147)  # Заявка отменена/отклонена
        inactive.remove(4)  # Отложенная
        with open(os.path.join(PF_BACKUP_DIRECTORY, 'inactive_statuses.json'), 'w') as write_file:
            json.dump(list(inactive), write_file, ensure_ascii=False)
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
            if not str(file['downloadLink']).startswith('https://finfort.planfix.ru'):
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


if __name__ == "__main__":
    request_count = 0
    limit_overflow = False
    files = {}
    reload_all()
    if os.path.exists(DIR4FILES):
        if not limit_overflow:
            # Копируем скачанные из АПИ JSON
            data_directory = os.path.join(DIR4JSONS, datetime.now().strftime("%Y-%m-%d"))
            os.mkdir(data_directory)
            for file in os.listdir(PF_BACKUP_DIRECTORY):
                shutil.copy(file, data_directory)

            # Обнуляем пути к файлу на диске
            for file in files:
                files[file]['full_path'] = ''  # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

            # Загружаем дерево проектов из сохраненного файла, добавляем безопасные для файловой системы названия проектов
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
                # qq = """
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
                # """
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
        else:
            print('\n', datetime.now().strftime("%H:%M:%S"), 'Неполное обновление - обновление файлов ПРОПУЩЕНО')
    else:
        print('\n', datetime.now().strftime("%H:%M:%S"), 'Не смонтирован диск с файлами - обновление файлов ПРОПУЩЕНО')
    print('\n', datetime.now().strftime("%H:%M:%S"),
          '============================ РАБОТА СКРИПТА ОКОНЧЕНА ============================', '\n\n')











