# -*- coding: utf-8 -*-
from flectra import http

# class HrEmployeePf(http.Controller):
#     @http.route('/hr_employee_pf/hr_employee_pf/', auth='public')
#     def index(self, **kw):
#         return "Hello, world"

#     @http.route('/hr_employee_pf/hr_employee_pf/objects/', auth='public')
#     def list(self, **kw):
#         return http.request.render('hr_employee_pf.listing', {
#             'root': '/hr_employee_pf/hr_employee_pf',
#             'objects': http.request.env['hr_employee_pf.hr_employee_pf'].search([]),
#         })

#     @http.route('/hr_employee_pf/hr_employee_pf/objects/<model("hr_employee_pf.hr_employee_pf"):obj>/', auth='public')
#     def object(self, obj, **kw):
#         return http.request.render('hr_employee_pf.object', {
#             'object': obj
#         })