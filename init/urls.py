from django.conf.urls import url

from . import views

urlpatterns = [
    url('about',views.about, name='about'),
    url('', views.index, name='index')

]
