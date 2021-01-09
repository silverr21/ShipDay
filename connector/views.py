from django.shortcuts import render
from django.views import View
from .helpers import get_cnxn, get_data

class Home(View):
    template_name = 'index.html'

    def get(self,request):
        data = get_data()
        print(data)
        return render(request, self.template_name, {})
