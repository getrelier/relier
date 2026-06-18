"""
Django views that dispatch Relier tasks.

Wire up in urls.py:
    path("users/<int:user_id>/avatar", views.AvatarUploadView.as_view()),
    path("users/<int:user_id>/export", views.DataExportView.as_view()),
    path("users/<int:user_id>/password-reset", views.PasswordResetView.as_view()),
    path("tasks/<str:task_id>", views.TaskStatusView.as_view()),
"""

from django.http import JsonResponse
from django.views import View

from .tasks import export_user_data, resize_avatar, send_password_reset


class AvatarUploadView(View):
    def post(self, request, user_id: int):
        # .push() is synchronous — safe inside a standard Django view
        image_path = request.FILES["avatar"].temporary_file_path()
        receipt = resize_avatar.push(user_id=user_id, image_path=image_path)
        return JsonResponse({"task_id": receipt.id}, status=202)


class DataExportView(View):
    def post(self, request, user_id: int):
        receipt = export_user_data.push(user_id=user_id)
        return JsonResponse({"task_id": receipt.id}, status=202)


class PasswordResetView(View):
    def post(self, request, user_id: int):
        token = request.POST.get("token", "")
        receipt = send_password_reset.push(user_id=user_id, token=token)
        return JsonResponse({"task_id": receipt.id}, status=202)


class TaskStatusView(View):
    def get(self, request, task_id: str):
        from relier.tasks.app import celery_app

        result = celery_app.AsyncResult(task_id)
        return JsonResponse(
            {
                "task_id": task_id,
                "status": result.state,
                "result": result.result if result.ready() else None,
            }
        )
