# Сверяет файлы в xml списке с файлами скачанными на диск и создает списки для скачивания и удаления

import json
import os

PF_BACKUP_DIRECTORY = 'current'
DIR4FILES = '/opt/PF_backup/files'
MAX_REPIT = 10
DELETE_FILES = True


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
    files_from_api = []
    with open(os.path.join(PF_BACKUP_DIRECTORY, 'files_full.json'), 'r') as read_file:
        files_loaded = json.load(read_file)
    for file in files_loaded:
        files_from_api.append(files_loaded[file]['full_path'])
    files_from_api = tuple(sorted(files_from_api))

    with open(os.path.join(PF_BACKUP_DIRECTORY, 'files_from_disk.json'), 'r') as read_file:
        files_from_disk = json.load(read_file)
    files_from_disk = tuple(sorted(files_from_disk))

    # Список на докачку
    printProgressBar(0, len(files_from_api), prefix='Проверено:', suffix='на загрузку', length=50)
    files4load = []
    for i, file in enumerate(files_from_api):
        printProgressBar(i, len(files_from_api), prefix='Проверено:', suffix='на загрузку', length=50)
        if file not in files_from_disk:
            files4load.append(file)

    # Список на удаление
    print('\n')
    printProgressBar(0, len(files_from_disk), prefix='Проверено:', suffix='на удаление', length=50)
    files4delete = []
    for i, file in enumerate(files_from_disk):
        printProgressBar(i, len(files_from_disk), prefix='Проверено:', suffix='на удаление', length=50)
        if file not in files_from_api:
            files4delete.append(file)

    q=0

