#   Copyright 2026 - 2026 wzycc
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
"""
Email Notification Module for Application Crash Alerts
用于软件启动失败时发送邮件通知的简单模块
"""

import smtplib
import logging
import socket
from email.mime.text import MIMEText
from email.utils import formatdate
from datetime import datetime
from typing import Optional, List
import traceback


class EmailNotifier:
    """简单的邮件通知类，支持纯文本邮件发送"""

    def __init__(
        self,
        smtp_server: str,
        smtp_port: int,
        sender_email: str,
        sender_password: str,
        recipients: List[str],
        use_tls: bool = True,
        timeout: int = 10
    ):
        """
        初始化邮件通知器

        Args:
            smtp_server: SMTP服务器地址 (如: smtp.gmail.com, smtp.163.com)
            smtp_port: SMTP端口 (如: 587, 465, 25)
            sender_email: 发件人邮箱
            sender_password: 发件人邮箱密码或授权码
            recipients: 收件人邮箱列表
            use_tls: 是否使用TLS加密
            timeout: 连接超时时间(秒)
        """
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.sender_email = sender_email
        self.sender_password = sender_password
        self.recipients = recipients if isinstance(recipients, list) else [recipients]
        self.use_tls = use_tls
        self.timeout = timeout

        # 设置日志
        self.logger = logging.getLogger(__name__)

    def send_notification(
        self,
        subject: str,
        body: str,
        include_timestamp: bool = True,
        include_hostname: bool = True
    ) -> bool:
        """
        发送纯文本邮件通知

        Args:
            subject: 邮件主题
            body: 邮件正文内容
            include_timestamp: 是否在主题中添加时间戳
            include_hostname: 是否在正文中添加主机名

        Returns:
            bool: 发送是否成功
        """
        # 未配置发件信息时静默跳过
        if not self.sender_email or not self.sender_password or not self.recipients:
            self.logger.debug("邮件未配置，跳过发送")
            return False
        try:
            # 构建邮件内容
            if include_timestamp:
                full_subject = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {subject}"
            else:
                full_subject = subject

            if include_hostname:
                hostname = socket.gethostname()
                full_body = f"主机: {hostname}\n时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n{body}"
            else:
                full_body = body

            # 创建纯文本邮件
            msg = MIMEText(full_body, 'plain', 'utf-8')
            msg['Subject'] = full_subject
            msg['From'] = self.sender_email
            msg['To'] = ', '.join(self.recipients)
            msg['Date'] = formatdate(localtime=True)

            # 连接SMTP服务器并发送
            with smtplib.SMTP(self.smtp_server, self.smtp_port, timeout=self.timeout) as server:
                if self.use_tls:
                    server.starttls()
                server.login(self.sender_email, self.sender_password)
                server.sendmail(self.sender_email, self.recipients, msg.as_string())

            self.logger.info(f"邮件通知发送成功: {full_subject}")
            return True

        except Exception as e:
            self.logger.error(f"邮件发送失败: {str(e)}")
            return False

    def send_error_notification(
        self,
        app_name: str,
        error: Exception,
        extra_info: Optional[str] = None
    ) -> bool:
        """
        发送应用启动失败的错误通知（专用方法）

        Args:
            app_name: 应用名称
            error: 捕获的异常对象
            extra_info: 额外的上下文信息

        Returns:
            bool: 发送是否成功
        """
        subject = f"【紧急】{app_name} 启动失败"

        # 构建错误详情
        error_trace = traceback.format_exc()
        body_lines = [
            f"应用名称: {app_name}",
            f"错误类型: {type(error).__name__}",
            f"错误信息: {str(error)}",
            ""
        ]

        if extra_info:
            body_lines.extend(["额外信息:", extra_info, ""])

        body_lines.extend([
            "详细堆栈跟踪:",
            "-" * 50,
            error_trace
        ])

        body = "\n".join(body_lines)
        return self.send_notification(subject, body)

    def set_email_template(self, subject_template: str, body_template: str):
        """
        设置邮件模板（目前仅存储，发送时不自动应用）

        Args:
            subject_template: 邮件主题模板，支持占位符如 {app_name}, {error_type}
            body_template: 邮件正文模板，支持占位符如 {app_name}, {error_type}, {error_message}, {stack_trace}
        """
        self.subject_template = subject_template
        self.body_template = body_template
    
    def send_template_notification(self, template_data: dict) -> bool:
        """
        使用预设模板发送邮件通知

        Args:
            template_data: 模板数据字典，包含占位符对应的值

        Returns:
            bool: 发送是否成功
        """
        if not hasattr(self, 'subject_template') or not hasattr(self, 'body_template'):
            self.logger.error("邮件模板未设置，请先调用 set_email_template()")
            return False
        
        try:
            subject = self.subject_template.format(**template_data)
            body = self.body_template.format(**template_data)
            return self.send_notification(subject, body)
        except KeyError as e:
            self.logger.error(f"模板数据缺少占位符: {str(e)}")
            return False
        except Exception as e:
            self.logger.error(f"使用模板发送邮件失败: {str(e)}")
            return False
        

def _load_email_config() -> dict:
    """从环境变量加载邮件配置，敏感信息不应硬编码在代码中"""
    import os

    smtp_server = os.environ.get("EMAIL_SMTP_SERVER", "smtp.example.com")
    smtp_port = int(os.environ.get("EMAIL_SMTP_PORT", "587"))
    use_tls = os.environ.get("EMAIL_USE_TLS", "true").lower() == "true"

    sender_email = os.environ.get("EMAIL_SENDER", "")
    sender_password = os.environ.get("EMAIL_PASSWORD", "")
    recipients_raw = os.environ.get("EMAIL_RECIPIENTS", "")
    recipients = [r.strip() for r in recipients_raw.split(",") if r.strip()]

    return {
        "smtp_server": smtp_server,
        "smtp_port": smtp_port,
        "sender_email": sender_email,
        "sender_password": sender_password,
        "recipients": recipients,
        "use_tls": use_tls,
    }


EMAIL_CONFIG = _load_email_config()

MAGIC_ERROR_KEYWORD = "JBLS7592"  # 用于识别错误邮件的特殊关键词
email_notifier = EmailNotifier(**EMAIL_CONFIG)
email_notifier.set_email_template(
    subject_template=f"[{MAGIC_ERROR_KEYWORD}] {{app_name}} 自动邮件通知",
    body_template=(
        "应用名称: {app_name}\n"
        "错误类型: {error_type}\n"
        "错误信息: {error_message}\n\n"
        "这封邮件自动发送，用于通知应用启动失败的相关信息。"
    )
)

def simple_send(error_type: str, error_message: str):
    """简化的发送函数，直接使用预设模板发送邮件"""
    email_notifier.send_template_notification({
        "app_name": "JobLens Trigger",
        "error_type": error_type,
        "error_message": error_message
    })

if __name__ == "__main__":
    # 检查配置是否已设置
    if not EMAIL_CONFIG.get("sender_email"):
        print("邮件未配置，请设置以下环境变量：")
        print("  EMAIL_SMTP_SERVER   SMTP服务器地址")
        print("  EMAIL_SMTP_PORT     SMTP端口（默认587）")
        print("  EMAIL_SENDER        发件人邮箱")
        print("  EMAIL_PASSWORD      发件人密码或授权码")
        print("  EMAIL_RECIPIENTS    收件人邮箱（逗号分隔）")
        print("  EMAIL_USE_TLS       是否启用TLS（默认true）")
    else:
        print("邮件配置已加载，尝试发送测试邮件...")
        email_notifier.send_template_notification({
            "app_name": "测试应用",
            "error_type": "测试错误",
            "error_message": "这是一个模拟的错误信息。"
        })