# -*- coding: utf-8 -*-
{
    'name': "users_pf",

    'summary': """
        Импорт юзеров из ПланФикса 
    """,

    'description': """
        Импорт юзеров из ПланФикса 
    """,

    'author': "Денис Алексеев",
    'website': "https://github.com/dekarh",

    # Categories can be used to filter modules in modules listing
    # Check https://github.com/flectra/flectra/blob/master/flectra/addons/base/module/module_data.xml
    # for the full list
    'category': 'Human Resources',
    'version': '0.0.3',

    # any module necessary for this one to work correctly
    'depends': ['base'],

    # always loaded
    'data': [
        # 'security/ir.model.access.csv',
        'views/views.xml',
        'data/res.users.csv',
    ],
    # only loaded in demonstration mode
    'demo': [
        'demo/demo.xml',
    ],
}