from django.shortcuts import render
from django.views import View
from .helpers import load_shipday_data

class Home(View):
    template_name = 'index.html'

    def get(self,request):
        data = load_shipday_data()
        #print(data)
        return render(request, self.template_name, {'rows':data})
