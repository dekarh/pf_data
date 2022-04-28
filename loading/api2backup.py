# Получаем из АПИ юзеров, группы, задачи, комментарии, список файлов и сопутствующую информацию

import json
import os
import requests
import xmltodict
from  sys import argv
from datetime import datetime, timedelta

from hide_data import USR_Tocken, PSR_Tocken, PF_ACCOUNT

URL = "https://apiru.planfix.ru/xml"
PF_BACKUP_DIRECTORY = 'current'
PF_HEADER = {"Accept": 'application/xml', "Content-Type": 'application/xml'}
RELOAD_ALL_FROM_API = True


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
                print(boost, datetime.now().strftime('%d.%m.%Y %H:%M:%S'),
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
    print('\n', datetime.now().strftime('%d.%m.%Y %H:%M:%S'),
          'Загружаем ранее полученный из АПИ список файлов. Потрачено запросов:', request_count)
    min_task = 18191034  # (89731) До этой задачи, задачи без файлов в дальнейшем не будут проверяться
    task_numbers_from_loaded_files = set()
    files = {}
    with open(os.path.join(PF_BACKUP_DIRECTORY, 'files_full.json'), 'r') as read_file:
        files_loaded = json.load(read_file)
    for file in files_loaded:
        files[int(file)] = files_loaded[file]
        if files[int(file)].get('task', None):
            if files[int(file)]['task'].get('id', None):
                task_numbers_from_loaded_files.add(int(files[int(file)]['task']['id']))
    task_numbers_from_loaded_files = tuple(task_numbers_from_loaded_files)

    print('\n', datetime.now().strftime('%d.%m.%Y %H:%M:%S'),
          'Загружаем бэкап задач (task.getMulti скорректированной task.get) через АПИ ПФ. Потрачено запросов:',
          request_count)
    tasks_full = {}
    all_tasks_ids = set()
    with open(os.path.join(PF_BACKUP_DIRECTORY, 'tasks_full.json'), 'r') as read_file:
        tasks_full_str = json.load(read_file)
    for task in tasks_full_str:
        all_tasks_ids.add(int(task))
        tasks_full[int(task)] = tasks_full_str[task]
    all_tasks_ids_tuple =  tuple(sorted(all_tasks_ids))
    print(datetime.now().strftime('%d.%m.%Y %H:%M:%S'), 'Из сохраненных полных (task.getMulti):', len(tasks_full))

    task_without_files = []
    tasks4check = []
    #task_without_files_general = []
    for task in all_tasks_ids_tuple:
        if task < min_task: #and task not in task_numbers_from_loaded_files:  # !!!!!!!!!!!!!!!!!! ВРЕМЕННО
            task_without_files.append(task)
            #task_without_files_general.append(int(tasks_full[task]['general']))
        else:
            tasks4check.append(task)
    task_without_files = tuple(task_without_files)
    #task_without_files_general = tuple(task_without_files_general)
    tasks4check = sorted(tasks4check)
    print('\n', datetime.now().strftime('%d.%m.%Y %H:%M:%S'), len(task_without_files),
          'задач без файлов в дальнейшем не будут проверяться')

    print('\n', datetime.now().strftime('%d.%m.%Y %H:%M:%S'),
          'Получаем из АПИ файлы по каждой задаче. Потрачено запросов:', request_count)
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
                print('Задача №', tasks_full[task].get('general', 'б/н'), '[', task, ']  (', i, 'из', len(tasks4check),
                      ') Потрачено запросов:', request_count)
        print('\n', datetime.now().strftime('%d.%m.%Y %H:%M:%S'),
              'Сохраняем результирующий список файлов')
        with open(os.path.join(PF_BACKUP_DIRECTORY, 'files_full.json'), 'w') as write_file:
            json.dump(files, write_file, ensure_ascii=False)

    print('\n', datetime.now().strftime('%d.%m.%Y %H:%M:%S'),
          'Получаем из АПИ юзеров, сотрудников, контакты, группы доступа и шаблоны задач. Потрачено запросов:',
          request_count)
    api_load_from_list('user.getList', 'user', 'users_full.json')
    api_load_from_list('contact.getList', 'contact', 'contacts_finfort.json' ,
                       api_additionally='<target>6532326</target>')
    contacts = api_load_from_list('contact.getList', 'contact', 'contacts_full.json')
    api_load_from_list('userGroup.getList', 'userGroup', 'usergroups_full.json')
    api_load_from_list('task.getList', 'task', 'tasktemplates_full.json',
                       api_additionally='<target>template</target>')

    print('\n', datetime.now().strftime('%d.%m.%Y %H:%M:%S'),
          'Получаем из АПИ дерево проектов (переименовал внутри flectra в hr.projectgroup)')
    projectgroups = api_load_from_list('project.getList', 'project', 'projectgroups_full.json')

    print('\n', datetime.now().strftime('%d.%m.%Y %H:%M:%S'),
          'Получаем из АПИ список файлов по каждому проекту. Потрачено запросов:', request_count)
    if not limit_overflow:
        if len(argv) == 1:
            printProgressBar(0, len(projectgroups) + 1, prefix='Скачаны все файлы по:', suffix='проектов', length=50)
        for i, projectgroup in enumerate(projectgroups):
            addition_text = '<project><id>' + str(projectgroup) + '</id></project>' \
                            + '<returnDownloadLinks>1</returnDownloadLinks>'
            files_loaded = api_load_from_list('file.getListForProject', 'file', '',
                                              api_additionally=addition_text)
            for file in files_loaded:
                files[file] = files_loaded[file]
            if len(argv) == 1:
                printProgressBar(i, len(projectgroups) + 1, prefix='Скачаны все файлы по:', suffix='проектов', length=50)

    print('\n', datetime.now().strftime('%d.%m.%Y %H:%M:%S'),
          'Получаем из АПИ список файлов по каждому контакту. Потрачено запросов:', request_count)
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
                printProgressBar(i, len(contacts) + 1, prefix='Скачаны все файлы по:', suffix='контактов', length=50)

    print('\n', datetime.now().strftime('%d.%m.%Y %H:%M:%S'),
          'Получаем из АПИ список справочников. Потрачено запросов:', request_count)
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
                printProgressBar(i, len(handbooks) + 1, prefix='Скачаны все записи по:', suffix='справочников', length=50)
        with open(os.path.join(PF_BACKUP_DIRECTORY, 'handbooks_full.json'), 'w') as write_file:
            json.dump(handbooks, write_file, ensure_ascii=False)

    print('\n', datetime.now().strftime('%d.%m.%Y %H:%M:%S'),
          'Получаем из АПИ список процессов и список статусов по каждому процессу. Потрачено запросов:', request_count)
    processes = api_load_from_list('taskStatus.getSetList', 'taskStatusSet', 'processes_full.json',
                                   pagination=False)
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

    print('\n', datetime.now().strftime('%d.%m.%Y %H:%M:%S'),
          'Загружаем бэкап комментариев')
    actions = {}
    with open(os.path.join(PF_BACKUP_DIRECTORY, 'actions_full.json'), 'r') as read_file:
        actions_str = json.load(read_file)
    for action in actions_str:
        actions[int(action)] = actions_str[action]
    print(datetime.now().strftime('%d.%m.%Y %H:%M:%S'), 'Из сохраненных комментариев:', len(actions))

    print(datetime.now().strftime('%d.%m.%Y %H:%M:%S'), 'Догружаем комментарии. Потрачено запросов:', request_count)
    addition_text = '<fromDate>' \
                    + (datetime.strptime(actions[max(actions.keys())]['dateTime'], '%d-%m-%Y %H:%M') -
                       timedelta(minutes=1)).strftime('%d-%m-%Y %H:%M') \
                    + '</fromDate><toDate>' \
                    + datetime.now().strftime('%d-%m-%Y %H:%M') \
                    + '</toDate><sort>asc</sort>'
    api_load_from_list('action.getListByPeriod', 'action', 'users_full.json',
                       api_additionally=addition_text, res_dict=actions)

    print('\n', datetime.now().strftime('%d.%m.%Y %H:%M:%S'),
          'Получаем из АПИ бэкап задач из выгрузки списка задач (task.getList). Потрачено запросов:', request_count)
    tasks_short = api_load_from_list('task.getList', 'task', 'tasks_short.json',
                                     api_additionally='<target>all</target>')
    for task in tasks_short:
        all_tasks_ids.add(int(task))
    all_tasks_ids_tuple =  tuple(sorted(all_tasks_ids))


    print('\n', datetime.now().strftime('%d.%m.%Y %H:%M:%S'),
          'Догружаем найденные задачи в полный бэкап tasks_full_from_api_backup')
    if not limit_overflow:
        not_finded_tasks_ids = set()
        deleted_tasks_ids = set()
        hundred4xml = []
        hundred_ids = []
        tasks_count = len(all_tasks_ids)
        tasks_full_checked = {}
        if len(argv) == 1:
            printProgressBar(0, tasks_count + 1, prefix='Скачано полных:', suffix='задач', length=50)
        try:
            for task in all_tasks_ids_tuple:
                if not tasks_full.get(task, None):
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
                                    for task_loaded in tasks_loaded:
                                        finded_ids = False
                                        for ids in hundred_ids:
                                            if int(task_loaded['id']) == ids:
                                                finded_ids = True
                                                tasks_full[ids] = task_loaded
                                        if not finded_ids:
                                            not_finded_tasks_ids.add(int(task_loaded['id']))
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
            print(datetime.now().strftime('%d.%m.%Y %H:%M:%S'), 'Всего везде:', len(all_tasks_ids),
                  'Сохранено:', len(tasks_full), 'Не найдено:', len(not_finded_tasks_ids))
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
                        elif xmltodict.parse(answer.text)['response']['@status'] == 'error' and xmltodict.parse(answer.text)['response']['code'] == '3001':
                            deleted_tasks_ids.add(task)
                            break
                        else:
                            if str(type(xmltodict.parse(answer.text)['response']['task'])).replace("'", '') == '<class NoneType>':
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
                print(datetime.now().strftime('%d.%m.%Y %H:%M:%S'), 'Удалено:', len(deleted_tasks_ids), 'осталось:', len(tasks_full_checked))
                with open(os.path.join(PF_BACKUP_DIRECTORY, 'tasks_full.json'), 'w') as write_file:
                        json.dump(tasks_full_checked, write_file, ensure_ascii=False)
    print('ВСЕГО потрачено запросов:', request_count)


if __name__ == "__main__":
    request_count = 0
    limit_overflow = False
    reload_all()







