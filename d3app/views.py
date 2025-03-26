from django.shortcuts import render
import csv
import json
from datetime import datetime
from django.http import HttpResponse
from django.views.decorators.http import require_POST
from django.views.decorators.csrf import csrf_exempt
from .models import Customer, ProductGroup, Product, Order, OrderDetail
from django.db.models import Sum, F, Func
from django.db.models.functions import ExtractMonth, ExtractWeekDay, ExtractDay, ExtractHour
from django.db import DatabaseError

@require_POST
@csrf_exempt
def import_csv(request):
    try:
        csv_file_path = 'data/data_ggsheet.csv'
        with open(csv_file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                name = row['Tên khách hàng'] if row['Tên khách hàng'] else None
                customer, _ = Customer.objects.get_or_create(
                    customer_id=row['Mã khách hàng'],
                    defaults={
                        'name': name,
                        'segment_code': row['Mã PKKH']
                    }
                )
                group, _ = ProductGroup.objects.get_or_create(
                    group_code=row['Mã nhóm hàng'],
                    defaults={'group_name': row['Tên nhóm hàng']}
                )
                try:
                    unit_price = int(row['Đơn giá'])
                except (ValueError, KeyError):
                    unit_price = 0
                product, _ = Product.objects.get_or_create( 
                    product_code=row['Mã mặt hàng'],
                    defaults={
                        'name': row['Tên mặt hàng'],
                        'group': group,
                        'unit_price': unit_price
                    }
                )
                order_time = datetime.strptime(row['Thời gian tạo đơn'], '%Y-%m-%d %H:%M:%S')
                order, _ = Order.objects.get_or_create(
                    order_id=row['Mã đơn hàng'],
                    defaults={
                        'customer': customer,
                        'order_time': order_time
                    }
                )
                order_detail, created = OrderDetail.objects.get_or_create(
                    order=order,
                    product=product,
                    defaults={'quantity': int(row['SL'])}
                )
                if not created:
                    order_detail.quantity += int(row['SL'])
                    order_detail.save()

        return HttpResponse("Data imported successfully!")
    except FileNotFoundError:
        return HttpResponse("Error: CSV file not found.", status=400)
    except Exception as e:
        return HttpResponse(f"Error: {str(e)}", status=500)


def data_visualization(request):
    try:
        # Q1 - Chỉ lấy các trường cần thiết
        order_details = OrderDetail.objects.select_related('order', 'product__group').values(
            'product__group__group_code',
            'product__group__group_name',
            'product__product_code',
            'product__name',
            'quantity',
            'product__unit_price'
        )
        data_for_q1 = [
            {
                'Mã nhóm hàng': detail['product__group__group_code'],
                'Tên nhóm hàng': detail['product__group__group_name'],
                'Mã mặt hàng': detail['product__product_code'],
                'Tên mặt hàng': detail['product__name'],
                'Thành tiền': detail['quantity'] * detail['product__unit_price'],
                'SL': detail['quantity']
            }
            for detail in order_details
        ]

        # Q2
        aggregated_data = OrderDetail.objects.values(
            'product__group__group_code',
            'product__group__group_name'
        ).annotate(
            SL=Sum('quantity'),
            Thành_tiền=Sum(F('quantity') * F('product__unit_price'))
        )
        data_for_q2 = [
            {
                'Mã nhóm hàng': item['product__group__group_code'],
                'Tên nhóm hàng': item['product__group__group_name'],
                'Thành tiền': item['Thành_tiền'],
                'SL': item['SL']
            }
            for item in aggregated_data
        ]

        # Q3
        month_data = OrderDetail.objects.values(
            month=ExtractMonth('order__order_time')
        ).annotate(
            SL=Sum('quantity'),
            Thành_tiền=Sum(F('quantity') * F('product__unit_price'))
        )
        data_for_q3 = [
            {
                'Tháng': f"{item['month']:02d}",
                'Thành tiền': item['Thành_tiền'],
                'SL': item['SL']
            }
            for item in month_data
        ]

        # Q4
        weekday_data = OrderDetail.objects.values(
            weekday=ExtractWeekDay('order__order_time'),
            date=Func('order__order_time', function='DATE')
        ).annotate(
            SL=Sum('quantity'),
            Thành_tiền=Sum(F('quantity') * F('product__unit_price'))
        )
        weekday_dict = {}
        for item in weekday_data:
            weekday = item['weekday']
            weekday_name = ["Chủ Nhật", "Thứ Hai", "Thứ Ba", "Thứ Tư", "Thứ Năm", "Thứ Sáu", "Thứ Bảy"][weekday - 1]
            if weekday_name not in weekday_dict:
                weekday_dict[weekday_name] = {'Thành tiền': 0, 'SL': 0, 'Ngày tạo đơn': set()}
            weekday_dict[weekday_name]['Thành tiền'] += item['Thành_tiền'] or 0
            weekday_dict[weekday_name]['SL'] += item['SL'] or 0
            weekday_dict[weekday_name]['Ngày tạo đơn'].add(str(item['date']))
        data_for_q4 = [
            {
                'Thứ': weekday_name,
                'Thành tiền': values['Thành tiền'],
                'SL': values['SL'],
                'Doanh số bán TB': values['Thành tiền'] / len(values['Ngày tạo đơn']) if values['Ngày tạo đơn'] else 0,
                'Số lượng bán TB': values['SL'] / len(values['Ngày tạo đơn']) if values['Ngày tạo đơn'] else 0
            }
            for weekday_name, values in weekday_dict.items()
        ]
        weekdays_order = ["Thứ Hai", "Thứ Ba", "Thứ Tư", "Thứ Năm", "Thứ Sáu", "Thứ Bảy", "Chủ Nhật"]
        data_for_q4.sort(key=lambda x: weekdays_order.index(x['Thứ']))
        # Q5
        day_data = OrderDetail.objects.values(
            day=ExtractDay('order__order_time'),
            date=Func('order__order_time', function='DATE')
        ).annotate(
            SL=Sum('quantity'),
            Thành_tiền=Sum(F('quantity') * F('product__unit_price'))
        )
        day_dict = {}
        for item in day_data:
            day = item['day']
            day_name = f"Ngày {day:02d}"
            if day_name not in day_dict:
                day_dict[day_name] = {'Thành tiền': 0, 'SL': 0, 'Ngày tạo đơn': set()}
            day_dict[day_name]['Thành tiền'] += item['Thành_tiền']
            day_dict[day_name]['SL'] += item['SL']
            day_dict[day_name]['Ngày tạo đơn'].add(str(item['date']))
        data_for_q5 = [
            {
                'Ngày trong tháng': day_name,
                'Thành tiền': values['Thành tiền'],
                'SL': values['SL'],
                'Doanh số bán TB': values['Thành tiền'] / len(values['Ngày tạo đơn']),
                'Số lượng bán TB': values['SL'] / len(values['Ngày tạo đơn'])
            }
            for day_name, values in day_dict.items()
        ]
        data_for_q5.sort(key=lambda x: int(x['Ngày trong tháng'].split(' ')[1]))

        # Q6
        hour_data = OrderDetail.objects.values(
            hour=ExtractHour('order__order_time'),
            date=Func('order__order_time', function='DATE')
        ).annotate(
            SL=Sum('quantity'),
            Thành_tiền=Sum(F('quantity') * F('product__unit_price'))
        )
        hour_dict = {}
        for item in hour_data:
            hour = item['hour']
            hour_name = f"{hour:02d}:00-{hour:02d}:59"
            if hour_name not in hour_dict:
                hour_dict[hour_name] = {'Thành tiền': 0, 'SL': 0, 'Ngày tạo đơn': set()}
            hour_dict[hour_name]['Thành tiền'] += item['Thành_tiền']
            hour_dict[hour_name]['SL'] += item['SL']
            hour_dict[hour_name]['Ngày tạo đơn'].add(str(item['date']))
        data_for_q6 = [
            {
                'Khung giờ': hour_name,
                'Thành tiền': values['Thành tiền'],
                'SL': values['SL'],
                'Doanh số bán TB': values['Thành tiền'] / len(values['Ngày tạo đơn']),
                'Số lượng bán TB': values['SL'] / len(values['Ngày tạo đơn'])
            }
            for hour_name, values in hour_dict.items()
        ]
        data_for_q6.sort(key=lambda x: int(x['Khung giờ'].split(':')[0]))

        # Q7
        order_group_data = OrderDetail.objects.values(
            'order__order_id',
            'product__group__group_code',
            'product__group__group_name'
        ).annotate(
            SL=Sum('quantity'),
            Thành_tiền=Sum(F('quantity') * F('product__unit_price'))
        )
        total_orders = Order.objects.count()
        group_dict = {}
        for item in order_group_data:
            group_name = item['product__group__group_name']
            group_code = item['product__group__group_code']
            full_group_name = f"[{group_code}] {group_name}"
            if full_group_name not in group_dict:
                group_dict[full_group_name] = {'Thành tiền': 0, 'SL': 0, 'Mã đơn hàng': set()}
            group_dict[full_group_name]['Thành tiền'] += item['Thành_tiền']
            group_dict[full_group_name]['SL'] += item['SL']
            group_dict[full_group_name]['Mã đơn hàng'].add(item['order__order_id'])
        data_for_q7 = [
            {
                'Nhóm hàng': group_name,
                'Thành tiền': values['Thành tiền'],
                'SL': values['SL'],
                'SL Đơn Bán': len(values['Mã đơn hàng']),
                'Xác suất bán': (len(values['Mã đơn hàng']) / total_orders) * 100 if total_orders > 0 else 0
            }
            for group_name, values in group_dict.items()
        ]
        data_for_q7.sort(key=lambda x: x['Xác suất bán'], reverse=True)

        # Q8
        order_month_group_data = OrderDetail.objects.values(
            'order__order_id',
            'order__order_time',
            'product__group__group_code',
            'product__group__group_name'
        ).annotate(
            SL=Sum('quantity'),
            Thành_tiền=Sum(F('quantity') * F('product__unit_price'))
        )
        total_orders_by_month = Order.objects.values(
            month=ExtractMonth('order_time')
        ).annotate(
            total_orders=Sum(1)
        )
        total_orders_dict = {f"Tháng {item['month']:02d}": item['total_orders'] for item in total_orders_by_month}
        month_group_dict = {}
        for item in order_month_group_data:
            month = f"Tháng {item['order__order_time'].strftime('%m')}"
            group_name = f"[{item['product__group__group_code']}] {item['product__group__group_name']}"
            key = f"{month}|{group_name}"
            if key not in month_group_dict:
                month_group_dict[key] = {
                    'Tháng': month,
                    'Nhóm hàng': group_name,
                    'Mã đơn hàng': set(),
                    'SL': 0,
                    'Thành tiền': 0
                }
            month_group_dict[key]['Mã đơn hàng'].add(item['order__order_id'])
            month_group_dict[key]['SL'] += item['SL']
            month_group_dict[key]['Thành tiền'] += item['Thành_tiền']
        data_for_q8 = [
            {
                'Tháng': values['Tháng'],
                'Nhóm hàng': values['Nhóm hàng'],
                'SL': values['SL'],
                'Thành tiền': values['Thành tiền'],
                'SL Đơn Bán': len(values['Mã đơn hàng']),
                'Xác suất bán': (len(values['Mã đơn hàng']) / total_orders_dict.get(values['Tháng'], 1)) * 100
            }
            for key, values in month_group_dict.items()
        ]
        data_for_q8.sort(key=lambda x: (x['Tháng'], x['Nhóm hàng']))

        # Q9
        order_detail_data = OrderDetail.objects.values(
            'order__order_id',
            'product__group__group_code',
            'product__group__group_name',
            'product__product_code',
            'product__name',
            'order__order_time'
        ).annotate(
            SL=Sum('quantity'),
            Thành_tiền=Sum(F('quantity') * F('product__unit_price'))
        )
        data_for_q9 = [
            {
                'Mã đơn hàng': item['order__order_id'],
                'Mã nhóm hàng': item['product__group__group_code'],
                'Tên nhóm hàng': item['product__group__group_name'],
                'Mã mặt hàng': item['product__product_code'],
                'Tên mặt hàng': item['product__name'],
                'Thành tiền': item['Thành_tiền'],
                'SL': item['SL'],
                'Thời gian tạo đơn': item['order__order_time'].strftime('%Y-%m-%d %H:%M:%S')
            }
            for item in order_detail_data
        ]

        # Q10
        order_detail_data_q10 = OrderDetail.objects.values(
            'order__order_id',
            'product__group__group_code',
            'product__group__group_name',
            'product__product_code',
            'product__name',
            'order__order_time'
        ).annotate(
            SL=Sum('quantity'),
            Thành_tiền=Sum(F('quantity') * F('product__unit_price'))
        )
        data_for_q10 = [
            {
                'Mã đơn hàng': item['order__order_id'],
                'Mã nhóm hàng': item['product__group__group_code'],
                'Tên nhóm hàng': item['product__group__group_name'],
                'Mã mặt hàng': item['product__product_code'],
                'Tên mặt hàng': item['product__name'],
                'Thành tiền': item['Thành_tiền'],
                'SL': item['SL'],
                'Thời gian tạo đơn': item['order__order_time'].strftime('%Y-%m-%d %H:%M:%S')
            }
            for item in order_detail_data_q10
        ]

        # Q11
        customer_order_data = Order.objects.values(
            'order_id',
            'customer__customer_id'
        )
        data_for_q11 = [
            {
                'Mã đơn hàng': item['order_id'],
                'Mã khách hàng': item['customer__customer_id'],
            }
            for item in customer_order_data
        ]

        # Q12
        customer_spending_data = OrderDetail.objects.values(
            'order__customer__customer_id'
        ).annotate(
            Thành_tiền=Sum(F('quantity') * F('product__unit_price'))
        )
        data_for_q12 = [
            {
                'Mã khách hàng': item['order__customer__customer_id'],
                'Thành tiền': item['Thành_tiền'],
            }
            for item in customer_spending_data
        ]

        return render(request, 'visualization.html', {
            'data_for_q1': json.dumps(data_for_q1, default=str),
            'data_for_q2': json.dumps(data_for_q2, default=str),
            'data_for_q3': json.dumps(data_for_q3, default=str),
            'data_for_q4': json.dumps(data_for_q4, default=str),
            'data_for_q5': json.dumps(data_for_q5, default=str),
            'data_for_q6': json.dumps(data_for_q6, default=str),
            'data_for_q7': json.dumps(data_for_q7, default=str),
            'data_for_q8': json.dumps(data_for_q8, default=str),
            'data_for_q9': json.dumps(data_for_q9, default=str),
            'data_for_q10': json.dumps(data_for_q10, default=str),
            'data_for_q11': json.dumps(data_for_q11, default=str),
            'data_for_q12': json.dumps(data_for_q12, default=str),
        })

    except DatabaseError as e:
        return HttpResponse(f"Database Error: {str(e)}", status=500)
    except Exception as e:
        return HttpResponse(f"Error: {str(e)}", status=500)