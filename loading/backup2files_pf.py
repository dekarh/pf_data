# Создает дерево директорий как дерево проектов ПФ и загружает в него все файлы ПФ по папкам с номером задачи
# Не используется т.к. имплементирован в api2backup.py
import sys

import requests
import xmltodict
import json
import os
from sys import getsizeof
from datetime import datetime
from  sys import argv
import shutil
from hide_data import USR_Tocken, PSR_Tocken, PF_ACCOUNT
from api2backup import api_load_from_list

URL = "https://apiru.planfix.ru/xml"
PF_HEADER = {"Accept": 'application/xml', "Content-Type": 'application/xml'}
PF_BACKUP_DIRECTORY = 'current'
DIR4FILES = '/opt/PF_backup/files'
MAX_REPIT = 10
DOWNLOAD_FILES = True


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


if __name__ == "__main__":
    # Загружаем список всех файлов
    files = {}
    with open(os.path.join(PF_BACKUP_DIRECTORY, 'files_full.json'), 'r') as read_file:
        files_loaded = json.load(read_file)
    for file in files_loaded:
        files[int(file)] = files_loaded[file]
        files[int(file)]['full_path'] = '' # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

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

    projects_in_levels = {0:[], 1: [], 2: [], 3: [], 4: [], 5: [], 6: [],}
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
                  '  ======================= УРОВЕНЬ', i, '===========================\n' )
            for j, project in enumerate(projects_in_levels[i]):
                print(datetime.now().strftime("%H:%M:%S"), 'УРОВЕНЬ', i, '|Проект', j + 1, 'из', len(projects_in_levels[i]))
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
                    rez_str = str(os.path.join(write_path, file['name'].replace('~&gt;', '').replace('&', ''). \
                                               replace(';', '').replace(':', '').replace('~', '').replace('/', ''). \
                                               replace('\\', '')))
                    if len(rez_str) > 230:
                        rez_str = rez_str[:224] + '.' + rez_str.split('.')[-1]
                    if DOWNLOAD_FILES:
                        rez_str, downloaded_files_ids = download_file(rez_str, downloaded_files_ids)
                    files[int(file['id'])]['full_path'] = rez_str
        # """
        print('\n', datetime.now().strftime("%H:%M:%S"),
              '  ======================= БЕЗ ПРОЕКТА ===========================\n' )
        without_project = set()
        j = 0
        for file in files.values():
            if not file.get('full_path', ''):
                if tasks_full.get(int(file['task']['id'])):
                    rez_str = os.path.join(DIR4FILES, '_Без_проекта', str(tasks_full[int(file['task']['id'])]['general']))
                else:
                    rez_str = os.path.join(DIR4FILES, '_Без_проекта',  file['task']['id'])
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
            if not os.path.exists(os.path.dirname(file.replace('/files/','/deleted/'))):
                os.makedirs(os.path.dirname(file.replace('/files/','/deleted/')), exist_ok=False)
            shutil.move(file, file.replace('/files/','/deleted/'))

    # Обновляем список загруженных на диск файлов
    files_from_disk = []
    for root, dirs, dir_files in os.walk(DIR4FILES):
        for file in dir_files:
            files_from_disk.append(os.path.join(root, file))
    with open(os.path.join(PF_BACKUP_DIRECTORY, 'files_from_disk.json'), 'w') as write_file:
        json.dump(files_from_disk, write_file, ensure_ascii=False)
    files_from_disk = tuple(sorted(files_from_disk))




