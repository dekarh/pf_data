# Загружаем задачи, комментарии и сопутствующую информацию из АПИ ПФ

import json
import os
import requests
import xmltodict
from lxml import etree, objectify
import csv
from datetime import datetime, timedelta

from lib import format_phone, l
from hide_data import USR_Tocken, PSR_Tocken, PF_ACCOUNT, DEPARTMENTS, OFFICETOWNS, DOMAIN
from api2backup import reload_all, api_load_from_list

URL = "https://apiru.planfix.ru/xml"
BACKUP_DIRECTORY = 'current'
PF_DATA = '../data'
DOCFLOW = '../../docflow/data'
PF_HEADER = {"Accept": 'application/xml', "Content-Type": 'application/xml'}
RELOAD_ALL_FROM_API = False
HAS_TASK_LIMIT = True
TASKS_FROM = 83000
TASKS_TO = 83020


def create_record(id, model, sources):
    """  Создаем запись БД flectra  """
    record = objectify.Element('record', id=id, model=model)
    fields = []
    i = -1
    for source in sources:
        if (str(source).endswith('_id') or str(source).endswith('_uid')) and sources[source]:
            i += 1
            fields.append(objectify.SubElement(record, 'field', name=source, ref=sources[source]))
        elif str(source).endswith('_ids') and str(type(sources[source])).find('list') > -1:
            i += 1
            parts = []
            for part in sources[source]:
                if part != '':
                    parts.append(part)
            if len(parts):
                fields.append(objectify.SubElement(
                    record,
                    'field',
                    name=source,
                    eval="[(6, 0, [ ref('" + "'), ref('".join(parts) + "')])]"))
            else:
                continue
        elif source and sources[source]:
            i += 1
            fields.append(objectify.SubElement(record, 'field', name=source))
            fields[i]._setText(str(sources[source]))
        else:
            pass
            # print(id, source, sources[source])
    return record


def dict_key(key, test_dict, process_index=False):
    """  Проверяет наличие test_dict[key]. Если есть - возвращает key, если нет - ''
    process_index=True - если есть, то возвращает строку, состояющую только из цифр
    """
    if test_dict.get(key ,None):
        if process_index:
            return processes4flectra[key]['name'].split('(')[1].split(')')[0]
        else:
            return key
    else:
        return ''

def check_parent_id(tid, tdict):
    if tid:
        if tdict.get(int(tdict[int(tid)]['parent']['id']), None):
            if int(tid) > int(tdict[int(tid)]['parent']['id']):
                return int(tdict[int(tid)]['parent']['id'])
            else:
                return None
        else:
            return None
    else:
        return None


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

def chk_users(id):
    if int(id) in users_ids:
        return str(id)
    else:
        return '5309784'


if __name__ == "__main__":
    # Перезагружаем всё из АПИ в файлы
    if RELOAD_ALL_FROM_API:
        request_count = 0
        limit_overflow = False
        reload_all()

    # backup2hr_pf():
    # Загружаем список групп и пустой список членов для каждой группы
    with open(os.path.join(BACKUP_DIRECTORY, 'usergroups_full.json'), 'r') as read_file:
        groups = json.load(read_file)
    groups_id2names = {}
    groups_id2members = {}
    for group in groups:
        groups_id2names[group] = groups[group]['name']
        groups_id2members[group] = []

    # Загружаем данные юзеров из файла
    with open(os.path.join(BACKUP_DIRECTORY, 'users_full.json'), 'r') as read_file:
        users_loaded = json.load(read_file)
    users_list = list(users_loaded.values())
    employees2mails = {}
    users2mails = {}
    users2groups = {}
    # Переводим в формат users_db[email], заполняем БД слияния контактов и юзеров:
    #                                                   employees2mails[e-mail] и users2mails[e-mail]
    for user in users_list:
        if user.get('email', None) or user.get('name', '') == 'робот ПланФикса':
            if user.get('name', '') == 'робот ПланФикса':
                user_email = 'robot_pf@odoo.' + DOMAIN
            else:
                if user['email'].find('_old@'+ DOMAIN) > -1:
                    user_email = user['email'].replace('_old@' + DOMAIN, '@odoo.' + DOMAIN)
                elif user['email'].find('@'+ DOMAIN) > -1:
                    user_email = user['email'].replace('@' + DOMAIN, '@odoo.' + DOMAIN)
                else:
                    user_email = user['email'].replace('_old@','@').split('@')[0] + '@odoo.' + DOMAIN
            if user['midName']:
                employees2mails[user_email] = {'name': str(user['lastName']) + ' ' + str(user['name']) + ' '
                                                    + str(user['midName'])}
                users2mails[user_email] = {'name': str(user['lastName']) + ' ' + str(user['name']) + ' '
                                                    + str(user['midName'])}
                users2groups[user_email] = set()
            else:
                employees2mails[user_email] = {'name': str(user['lastName']) + ' ' + str(user['name'])}
                users2mails[user_email] = {'name': str(user['lastName']) + ' ' + str(user['name'])}
                users2groups[user_email] = set()
            users2mails[user_email]['login'] = user_email
            employees2mails[user_email]['work_email'] = user_email
            if user.get('phones', None):
                if user['phones']:
                    if str(type(user['phones']['phone'])).find('list') > -1:
                        employees2mails[user_email]['mobile_phone'] = \
                            str(format_phone(user['phones']['phone'][0]['number']))
                    else:
                        employees2mails[user_email]['mobile_phone'] = str(format_phone(user['phones']['phone']['number']))
            if user.get('sex', None):
                employees2mails[user_email]['gender'] = str(user['sex']).lower()
            if user.get('id', None):
                users2mails[user_email]['id_pf'] = str(user['id'])
            if user.get('general', None):
                users2mails[user_email]['general_user_pf'] = str(user['general'])
            if user.get('active', None):
                users2mails[user_email]['active'] = str(user['status'] == 'ACTIVE')
            if user.get('userGroups', None):
                if str(type(user['userGroups']['userGroup'])).find('list') > -1:
                    for group in user['userGroups']['userGroup']:
                        groups_id2members[group['id']] += [user_email]
                        users2groups[user_email].add(group['id'])
                else:
                    groups_id2members[user['userGroups']['userGroup']['id']] += [user_email]
        else:
            print(str(user['id']), str(user['lastName']), str(user['name']), str(user['midName']), ' - нет e-mail')

    # Загружаем данные сотрудников из файла
    with open(os.path.join(BACKUP_DIRECTORY, 'contacts_' + PF_ACCOUNT + '.json'), 'r') as read_file:
        contacts_loaded = json.load(read_file)
    contacts_list = list(contacts_loaded.values())
    contacts_db = {}
    # Переводим в формат users_db[email], заполняем БД сляния контактов и юзеров employees2mails[e-mail]
    for contact in contacts_list:
        mail = ''
        for field in contact['customData']['customValue']:
            if field['field']['name'] == 'Корпоративная почта':
                if field['text'].find('_old@' + DOMAIN) > -1:
                    mail = field['text'].replace('_old@' + DOMAIN, '@odoo.' + DOMAIN)
                elif field['text'].find('@' + DOMAIN) > -1:
                    mail = field['text'].replace('@' +DOMAIN, '@odoo.' + DOMAIN)
                else:
                    mail = field['text'].replace('_old@','@').split('@')[0] + '@odoo.' + DOMAIN
        if mail:
            contacts_db[mail] = contact
            if not employees2mails.get(mail, None):
                employees2mails[mail] = {}
                users2mails[mail] = {'login': mail}
                employees2mails[mail]['work_email'] = mail
            users2mails[mail]['general_contact_pf'] = contact['general']
            users2mails[mail]['userid_pf'] = contact['userid']
            for field in contact['customData']['customValue']:
                if field['field']['name'] == 'ФИО':
                    if employees2mails[mail].get('name', None):
                        if len(str(field['text']).strip().split(' ')) > \
                              len(str(employees2mails[mail]['name']).strip().split(' ')):
                            employees2mails[mail]['name'] = field['text']
                            users2mails[mail]['name'] = field['text']
                    else:
                        employees2mails[mail]['name'] = field['text']
                        users2mails[mail]['name'] = field['text']
                #if field['field']['name'] == 'Город':
                #if field['field']['name'] == 'д/р сотрудника':
                if field['field']['name'] == 'Статус':
                    employees2mails[mail]['status'] = field['text']
                    users2mails[mail]['active'] = str(field['text'] == 'Активный')
                    employees2mails[mail]['active'] = str(field['text'] == 'Активный')
                if field['field']['name'] == 'Подразделение (отдел)' and field['text']:
                    if field['text'] == 'ПродБлок':
                        field['text'] = 'Продуктовый блок'
                    employees2mails[mail]['department_id'] = 'department_' +  str(DEPARTMENTS.index(str(field['text'])))
        else:
            print(str(contact['id']), str(contact['general']), ' - нет e-mail')

    i = 1
    for mail in users2mails:
        if users2mails[mail].get('id_pf', None):
            users2mails[mail]['id'] = 'user_' + users2mails[mail]['id_pf']
        elif users2mails[mail].get('userid_pf', None):
            users2mails[mail]['id'] = 'user_' + users2mails[mail]['userid_pf']
        else:
            users2mails[mail]['id'] = 'user_' + str(i)
            i += 1
        if users2groups.get(mail, None):
            if len(users2groups[mail]):
                users2mails[mail]['groups_id:id'] = ''
                for group in users2groups[mail]:
                    users2mails[mail]['groups_id:id'] += 'docflow.' + group + ','
                users2mails[mail]['groups_id:id'] = users2mails[mail]['groups_id:id'].strip(',')

    i = 1
    employees4flectra = {}
    employees4backup = {}
    for mail in employees2mails:
        if users2mails.get(mail, None):
            if users2mails[mail].get('id_pf', None):
                employees4flectra['empl_' + users2mails[mail]['id_pf']] = employees2mails[mail]
                employees4flectra['empl_' + users2mails[mail]['id_pf']]['id_pf'] = users2mails[mail]['id_pf']
                employees4backup[users2mails[mail]['id_pf']] = employees2mails[mail]
                employees4backup[users2mails[mail]['id_pf']]['id_pf'] = users2mails[mail]['id_pf']
            elif users2mails[mail].get('userid_pf', None):
                employees4flectra['empl_' + users2mails[mail]['userid_pf']] = employees2mails[mail]
                employees4flectra['empl_' + users2mails[mail]['userid_pf']]['id_pf'] = users2mails[mail]['userid_pf']
                employees4backup[users2mails[mail]['userid_pf']] = employees2mails[mail]
                employees4backup[users2mails[mail]['userid_pf']]['id_pf'] = users2mails[mail]['userid_pf']
            else:
                employees4flectra['empl_' + str(i)] = employees2mails[mail]
                employees4flectra['empl_' + str(i)]['id_pf'] = str(i)
                employees4backup[str(i)] = employees2mails[mail]
                employees4backup[str(i)]['id_pf'] = str(i)
                i += 1
    with open(os.path.join(BACKUP_DIRECTORY, 'emloyees_' + PF_ACCOUNT + '.json'), 'w') as write_file:
            json.dump(employees4backup, write_file, ensure_ascii=False)

    # Заголовок xml
    flectra_root = objectify.Element('flectra')
    flectra_data = objectify.SubElement(flectra_root, 'data')

    # Базовые департаменты, ГД, ЗГД
    record = create_record('department_gd', 'hr.department', {'name': 'Генеральный директор'})
    flectra_data.append(record)
    record = create_record('department_zgd', 'hr.department', {
        'name': 'Заместитель генерального директора',
        'parent_id': 'department_gd',
    })
    flectra_data.append(record)
    for i, department in enumerate(DEPARTMENTS):  # DEPARTMENTS - list с именами департаментов
        record = create_record('department_' + str(i), 'hr.department', {
            'name': department,
            'parent_id': 'department_zgd',
        })
        flectra_data.append(record)

    # Справочник офисов-городов
    for i, officetown in enumerate(OFFICETOWNS):   # OFFICETOWNS - list с именами офисегородов
        record = create_record('officetown_' + str(i), 'hr_pf.officetown', {'name': officetown})
        flectra_data.append(record)

    # Группы доступа, сначала корневая группа "Планфикс" в .xml
    record = create_record('category_pf', 'ir.module.category', {'name': 'ПланФикс'})
    flectra_data.append(record)
    groups4csv = {}
    for groups_id2name in groups_id2names:
        groups4csv[groups_id2name] = {'id': str(groups_id2name), 'name': groups_id2names[groups_id2name]}
        record = create_record(
            'docflow.' + str(groups_id2name),
            'res.groups',
            {
                'category_id': 'category_pf',
                'id_from_pf': str(groups_id2name)}
        )
        flectra_data.append(record)
    # Полученные группы в .csv
    with open(os.path.join(DOCFLOW, 'res.groups.csv'), 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=['id', 'name'])
        writer.writeheader()
        for group in groups4csv:
            writer.writerow(groups4csv[group])

    # Юзеры в .csv в два файла-этапа загрузки
    users4csv = {}
    for mail in users2mails:
        users4csv[mail] = {}
        for field in users2mails[mail].keys():
            if field in ['id', 'name', 'login', 'active']:
                users4csv[mail][field] = users2mails[mail][field]
    with open(os.path.join(DOCFLOW, 'res.users.csv'), 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=['id', 'name', 'login', 'active'])
        writer.writeheader()
        for mail in users2mails:
            writer.writerow(users4csv[mail])
    users4csv = {}
    for mail in users2mails:
        users4csv[mail] = {}
        for field in users2mails[mail].keys():
            if field in ['id', 'id_pf', 'general_user_pf', 'general_contact_pf', 'userid_pf', 'groups_id:id']:
                if field == 'id':
                    users4csv[mail][field] = 'docflow.' + users2mails[mail][field]
                else:
                    users4csv[mail][field] = users2mails[mail][field]
    with open(os.path.join(PF_DATA, 'res.users.csv'), 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=['id', 'id_pf', 'general_user_pf', 'general_contact_pf',
                                                     'userid_pf', 'groups_id:id'])
        writer.writeheader()
        for mail in users2mails:
            writer.writerow(users4csv[mail])

    # Контакты сотрудников в res.partners.csv + указываем что это не customer не supplier
    partners4csv = {}
    for mail in users2mails:
        partners4csv[mail] = {}
        for field in users2mails[mail].keys():
            if field in ['id', 'is_company', 'customer', 'supplier']:
                if field == 'id':
                    partners4csv[mail][field] = 'docflow.' + users2mails[mail][field] + '_res_partner'
                    partners4csv[mail]['email'] = mail
                    partners4csv[mail]['is_company'] = False
                    partners4csv[mail]['customer'] = False
                    partners4csv[mail]['supplier'] = False
    with open(os.path.join(PF_DATA, 'res.partner.csv'), 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=['id', 'email', 'is_company', 'customer', 'supplier'])
        writer.writeheader()
        for mail in users2mails:
            writer.writerow(partners4csv[mail])

    # Сотрудники в .xml
    for emloyee in employees4flectra:
        record = create_record(emloyee, 'hr.employee', employees4flectra[emloyee])
        flectra_data.append(record)

    # удаляем все lxml аннотации.
    objectify.deannotate(flectra_root)
    etree.cleanup_namespaces(flectra_root)

    # конвертируем все в привычную нам xml структуру.
    obj_xml = etree.tostring(flectra_root,
                             pretty_print=True,
                             xml_declaration=True,
                             encoding='UTF-8'
                             )

    try:
        with open(os.path.join(PF_DATA, 'hr_pf_data.xml'), "wb") as xml_writer:
            xml_writer.write(obj_xml)
    except IOError:
        pass

    #  backup2project()

    # Загружаем бэкап задач из выгрузки всех задач (task.getMulti скорректированной task.get) через АПИ ПФ
    tasks_full = {}
    with open(os.path.join(BACKUP_DIRECTORY, 'tasks_full.json'), 'r') as read_file:
        tasks_full_str = json.load(read_file)
    for task in tasks_full_str:
        tasks_full[int(task)] = tasks_full_str[task]
    print('Из сохраненных полных (task.getMulti):', len(tasks_full))

    # id загруженных в модуле hr_pf юзеров и сотрудников
    with open(os.path.join(BACKUP_DIRECTORY, 'users_full.json'), 'r') as read_file:
        users_loaded = json.load(read_file)
    users_ids = tuple([int(x) for x in users_loaded.keys()])

    # Процессы (project.project)
    with open(os.path.join(BACKUP_DIRECTORY, 'processes_full.json'), 'r') as read_file:
        processes = json.load(read_file)
    processes4flectra = {}
    for process in processes:
        processes4flectra['pr_' + str(process)] = {
            'name': processes[process]['name'],
            'id_pf': process,
        }

    # Статусы
    with open(os.path.join(BACKUP_DIRECTORY, 'statuses_flectra.json'), 'r') as read_file:
        statuses4flectra = json.load(read_file)

    # Шаблоны задач
    with open(os.path.join(BACKUP_DIRECTORY, 'tasktemplates_full.json'), 'r') as read_file:
        tasktemplates = json.load(read_file)
    tasktemplates4flectra = {}
    for tasktemplate in tasktemplates:
        tasktemplates4flectra['tt_' + str(tasktemplate)] = {
            'name': tasktemplates[tasktemplate]['title'],
            'id_pf': tasktemplates[tasktemplate]['id'],
            'id_pf_general': tasktemplates[tasktemplate]['general']
        }

    # Загружаем данные по задачам из файла, полученного из АПИ
    tasks_from_json = []
    if HAS_TASK_LIMIT:
        tasks_from_json_ids = tuple(sorted(list(tasks_full.keys()))[TASKS_FROM:TASKS_TO])
    else:
        tasks_from_json_ids = tuple(sorted(list(tasks_full.keys())))
    for task in tasks_from_json_ids:
        tasks_from_json.append(tasks_full[task])

    # Названия полей и id загружаем в процессе
    template_fields = {}
    fields_fills = {}
    stages = {}
    tasks = {}
    tasks_from_json_dict = {}
    for task in tasks_from_json:
        if task['type'] == 'task':
            tasks_from_json_dict[int(task['id'])] = task
            tasks['task_' + task['id']] = {
                'id_pf': task['id'],
                'id_pf_general': task['general'],
                'name': task['title'],
                'description': task['description'],
                'project_id': dict_key('pr_' + task['statusSet'], processes4flectra),
                'stage_id': dict_key(
                    'st_' + task['status'] + dict_key('pr_' + task['statusSet'], processes4flectra, process_index=True),
                    statuses4flectra),
                'create_date': str(task['beginDateTime']).replace('-', '.') + ':00',
                'user_id': 'docflow.user_' + chk_users(task['owner']['id']),
                'employee_id':  'pf_data.empl_' + chk_users(task['owner']['id']),
                'tasktemplate_id': dict_key('tt_' + task['template']['id'], tasktemplates4flectra),
            }
            if task.get('importance', None):
                if task['importance'] == 'HIGH':
                    tasks['task_' + task['id']]['priority'] = '0'
                else:
                    tasks['task_' + task['id']]['priority'] = '2'
            else:
                tasks['task_' + task['id']]['priority'] = '2'
            if task.get('startTime', None):
                if len(str(task['startTime'])) < 16:
                    tasks['task_' + task['id']]['date_start'] = \
                        str(task['startTime']).strip(' ')[:10].replace('-', '.') + ' 09:00:00'
                else:
                    tasks['task_' + task['id']]['date_start'] = str(task['startTime']).replace('-', '.') + ':00'
            if task.get('endTime', None):
                if len(str(task['endTime'])) < 16:
                    tasks['task_' + task['id']]['date_deadline'] = \
                        str(task['endTime']).strip(' ')[:10].replace('-', '.') + ' 09:00:00'
                else:
                    tasks['task_' + task['id']]['date_deadline'] = str(task['endTime']).replace('-', '.') + ':00'
            if task.get('duration', None) and task.get('durationUnit', None):
                if task['durationUnit'] == 0:
                    tasks['task_' + task['id']]['planned_hours'] = str(int(task['duration'])*60)
                elif task['durationUnit'] == 1:
                    tasks['task_' + task['id']]['planned_hours'] = task['duration']
                elif task['durationUnit'] == 2:
                    tasks['task_' + task['id']]['planned_hours'] = str(int(task['duration'])/24)

    for task in tasks_from_json:
        if task['type'] == 'task':
            down_recursion = True
            project_ids = [int(task['id'])]
            current_id = task['id']
            while down_recursion:
                parent_id = check_parent_id(current_id, tasks_from_json_dict)
                if parent_id:
                    tasks['task_' + task['id']]['parent_id'] = 'task_' + str(parent_id)
                    project_ids.append(parent_id)
                    current_id = parent_id
                else:
                    down_recursion = False
            project_ids.sort()
            for task_id in project_ids:
                tasks['task_' + str(task_id)]['project_id'] = tasks['task_' + str(project_ids[0])]['project_id']
            if task.get('customData', None):
                if task['customData'].get('customValue', None):
                    if str(type(task['customData']['customValue'])).find('list') > -1:
                        for field in task['customData']['customValue']:
                            if not template_fields.get('tpl_field_' + field['field']['id']):
                                template_fields['tpl_field_' + field['field']['id']] = {
                                    'name': field['field']['name'],
                                    'id_pf': field['field']['id']
                                }
                    else:
                        field = task['customData']['customValue']
                        if not template_fields.get('tpl_field_' + field['field']['id']):
                            template_fields['tpl_field_' + field['field']['id']] = {
                                'name': field['field']['name'],
                                'id_pf': field['field']['id']
                            }
    for task in tasks_from_json:
        if task['type'] == 'task':
            if task.get('customData', None):
                if task['customData'].get('customValue', None):
                    if str(type(task['customData']['customValue'])).find('list') > -1:
                        for field in task['customData']['customValue']:
                            fields_fills[task['id'] + '_' + field['field']['id']] = {
                                'task_id': dict_key('task_' +  task['id'], tasks),
                                'template_field_name_id': dict_key('tpl_field_' + field['field']['id'], template_fields),
                                'text': field.get('text',''),
                                'value': field.get('value',''),
                            }
                    else:
                        field = task['customData']['customValue']
                        fields_fills[task['id'] + '_' + field['field']['id']] = {
                            'task_id': 'task_' + task['id'],
                            'template_field_name_id': 'tpl_field_' + field['field']['id'],
                            'text': field.get('text', ''),
                            'value': field.get('value', ''),
                        }

    # Заголовок xml
    flectra_root = objectify.Element('flectra')
    flectra_data = objectify.SubElement(flectra_root, 'data')

    # Процессы (project.project)
    for process in processes4flectra:
        record = create_record(process, 'project.project', processes4flectra[process])
        flectra_data.append(record)

    # Статусы (project.task.type)
    for status in statuses4flectra:
        record = create_record(status, 'project.task.type', statuses4flectra[status])
        flectra_data.append(record)

    # Шаблоны полей (docflow.field.template)
    for template_field in template_fields:
        record = create_record(str(template_field), 'docflow.field.template', template_fields[template_field])
        flectra_data.append(record)

    # Шаблоны задач (docflow.tasktemplate)
    for tasktemplate in tasktemplates4flectra:
        record = create_record(tasktemplate, 'docflow.tasktemplate', tasktemplates4flectra[tasktemplate])
        flectra_data.append(record)

    # Задачи (project.task)
    for i in range(min(tasks_full.keys())-10,max(tasks_full.keys())+10):
        if tasks.get('task_' + str(i), None):
            record = create_record('task_' + str(i), 'project.task', tasks['task_' + str(i)])
            flectra_data.append(record)

    # Поля (docflow.field)
    for field_fill in fields_fills:
        record = create_record(field_fill, 'docflow.field', fields_fills[field_fill])
        flectra_data.append(record)

    # удаляем все lxml аннотации.
    objectify.deannotate(flectra_root)
    etree.cleanup_namespaces(flectra_root)

    # конвертируем все в привычную нам xml структуру.
    obj_xml = etree.tostring(flectra_root,
                             pretty_print=True,
                             xml_declaration=True,
                             encoding='UTF-8'
                             )

    try:
        with open(os.path.join(PF_DATA, 'docflow_data.xml'), "wb") as xml_writer:
            xml_writer.write(obj_xml)
    except IOError:
        pass

    # files() && actions()
    # Загружаем список всех файлов
    files = {}
    with open(os.path.join(BACKUP_DIRECTORY, 'files_full.json'), 'r') as read_file:
        files_loaded = json.load(read_file)
    for file in files_loaded:
        if files.get(int(file),False):  # Если есть такой же файл меньшей версии - замещаем
            if files[int(file)]['version'] < files_loaded[file]['version']:
                files[int(file)] = files_loaded[file]
        else:
            files[int(file)] = files_loaded[file]

    files4flectra = {}
    location_count = 0   #
    for file in files:
        if HAS_TASK_LIMIT:
            if int(files[file].get('task', {}).get('id', 0)) not in tasks_from_json_ids:
                continue
        if files[file]['sourceType'] in ['FILESYSTEM', 'DOCSTEMPLATE']:
            if files[file].get('full_path', False):
                files4flectra['att_' + str(file)] = {
                    'name': files[file]['name'],
                    'datas_fname': files[file]['name'],
                    'file_id_pf': file,
                    'file_path_pf': files[file]['full_path'],
                    'type': 'binary'
                }
            else:
                continue
        elif files[file]['sourceType'] == 'INTERNET':
            if files[file].get('downloadLink'):
                files4flectra['att_' + str(file)] = {
                    'name': files[file]['name'],
                    'file_id_pf': file,
                    'url': files[file]['downloadLink'],
                    'type': 'url'
                }
            else:
                continue
        else:
            continue
        if files[file]['description']:
            files4flectra['att_' + str(file)]['description'] = files[file]['description']
        if files[file].get('project', False):
            if files[file]['project'].get('id', False):
                if int(files[file]['project']['id']):
                    files4flectra['att_' + str(file)]['project_id_external'] = 'pr_' + files[file]['project']['id']
        if files[file].get('task', False):
            if files[file]['task'].get('id', False):
                if int(files[file]['task']['id']):
                    files4flectra['att_' + str(file)]['task_id_external'] = 'task_' + files[file]['task']['id']
        #if files[file].get('user', False):
        #    if files[file]['user'].get('id', False):
        #        files4flectra['att_' + str(file)]['create_uid'] = 'docflow.user_' + chk_users(files[file]['user']['id'])
        files4flectra['att_' + str(file)]['file_version_pf'] = files[file]['version']

    # Загружаем бэкап комментариев
    actions4flectra = {}
    actions = {}
    with open(os.path.join(BACKUP_DIRECTORY, 'actions_full.json'), 'r') as read_file:
        actions_str = json.load(read_file)
    for action in actions_str:
        actions[int(action)] = actions_str[action]
    print('Из сохраненных комментариев:', len(actions))

    # Формируем комментарии для вывода в Flectra
    for action in actions:
        if int(actions[action]['task']['id']) in tasks_from_json_ids:
            actions4flectra['msg_' + str(actions[action]['id'])] = {
                'date': actions[action]['dateTime'] + ':00',
                'author_id': 'docflow.user_' + chk_users(actions[action]['owner']['id']) + '_res_partner',
                'res_id': 'task_' + str(actions[action]['task']['id']),
                'body': actions[action]['description'],
                'message_type': 'comment',
                'model': 'project.task'
            }
            if len(actions[action].get('files', {}).get('file',[])):
                if str(type(actions[action]['files']['file'])).replace("'","") == '<class dict>':
                    actions4flectra['msg_' + str(actions[action]['id'])]['attachment_ids'] = \
                        [dict_key('att_' + actions[action]['files']['file']['id'], files4flectra)]
                else:
                    actions4flectra['msg_' + str(actions[action]['id'])]['attachment_ids'] = [
                        dict_key('att_' + actions[action]['files']['file'][x]['id'], files4flectra)
                        for x in range(len(actions[action]['files']['file']))
                    ]

    # Заголовок xml
    flectra_root = objectify.Element('flectra')
    flectra_data = objectify.SubElement(flectra_root, 'data')

    # Комментарии
    for action in actions4flectra:
        record = create_record(action, 'mail.message', actions4flectra[action])
        flectra_data.append(record)

    off = """
    # Файлы (ir.attachment)
    for file in files4flectra:
        record = create_record(file, 'ir.attachment', files4flectra[file])
        flectra_data.append(record)
    """

    # удаляем все lxml аннотации.
    objectify.deannotate(flectra_root)
    etree.cleanup_namespaces(flectra_root)

    # конвертируем все в привычную нам xml структуру.
    obj_xml = etree.tostring(flectra_root,
                             pretty_print=True,
                             xml_declaration=True,
                             encoding='UTF-8'
                             )

    try:
        with open(os.path.join(PF_DATA, 'actions_data.xml'), "wb") as xml_writer:
            xml_writer.write(obj_xml)
    except IOError:
        pass


    with open(os.path.join(PF_DATA, 'ir.attachment.csv'), 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=[ 'id', 'name', 'file_id_pf', 'file_path_pf', 'type', 'url',
                                                      'description', 'project_id_external', 'task_id_external',
                                                      'datas_fname', 'file_version_pf'])
        writer.writeheader()
        for file in files4flectra:
            files4flectra[file].update({'id': file})
            writer.writerow(files4flectra[file])


