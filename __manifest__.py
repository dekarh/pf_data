# -*- coding: utf-8 -*-
{
    'name': "pf_data",

    'summary': """
       Импорт данных из API ПланФикса для модуля docflow 
    """,

    'description': """
       Импорт данных из API ПланФикса для модуля docflow 
        (документооборот для flectra в стиле Планфикс) 
    """,

    'author': "Денис Алексеев",
    'website': "https://github.com/dekarh",

    # Categories can be used to filter modules in modules listing
    # Check https://github.com/flectra/flectra/blob/master/flectra/addons/base/module/module_data.xml
    # for the full list
    'category': 'Sign',
    'version': '0.0.17',

    # any module necessary for this one to work correctly
    'depends': ['base', 'project', 'docflow'],

    # always loaded
    'data': [
        # 'security/ir.model.access.csv',
        'views/views.xml',
        'data/hr_pf_data.xml',
        'data/docflow_data.xml',
        'data/res.users.csv',
        'data/res.partner.csv',
        'data/ir.attachment.csv',
        'data/actions_data.xml',
    ],
    # only loaded in demonstration mode
    'demo': [
        'demo/demo.xml',
    ],
}
