# d3app/urls.py
from django.urls import path
from . import views

urlpatterns = [
    path('', views.data_visualization, name='data_visualization'), 
    path('import/', views.import_csv, name='import_csv'), 
