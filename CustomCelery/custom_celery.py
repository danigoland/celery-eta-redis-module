from celery import Celery
from celery.utils.functional import maybe_list
from kombu.utils.uuid import uuid
import warnings
from celery.exceptions import AlwaysEagerIgnored
import json
import base64


class CustomCelery(Celery):
    def send_task(self, name, args=None, kwargs=None, countdown=None,
                  eta=None, task_id=None, producer=None, connection=None,
                  router=None, result_cls=None, expires=None,
                  publisher=None, link=None, link_error=None,
                  add_to_parent=True, group_id=None, group_index=None,
                  retries=0, chord=None,
                  reply_to=None, time_limit=None, soft_time_limit=None,
                  root_id=None, parent_id=None, route_name=None,
                  shadow=None, chain=None, task_type=None, **options):
        if not countdown:
            return super(self).send_task(name, args=args, kwargs=kwargs, countdown=countdown,
                                         eta=eta, task_id=task_id, producer=producer, connection=connection,
                                         router=router, result_cls=result_cls, expires=expires,
                                         publisher=publisher, link=link, link_error=link_error,
                                         add_to_parent=add_to_parent, group_id=group_id, group_index=group_index,
                                         retries=retries, chord=chord,
                                         reply_to=reply_to, time_limit=time_limit, soft_time_limit=soft_time_limit,
                                         root_id=root_id, parent_id=parent_id, route_name=route_name,
                                         shadow=shadow, chain=chain, task_type=task_type, **options)

        parent = have_parent = None
        amqp = self.amqp
        task_id = task_id or uuid()
        producer = producer or publisher  # XXX compat
        router = router or amqp.router
        conf = self.conf
        if conf.task_always_eager:  # pragma: no cover
            warnings.warn(AlwaysEagerIgnored(
                'task_always_eager has no effect on send_task',
            ), stacklevel=2)

        ignored_result = options.pop('ignore_result', False)
        options = router.route(
            options, route_name or name, args, kwargs, task_type)

        if not root_id or not parent_id:
            parent = self.current_worker_task
            if parent:
                if not root_id:
                    root_id = parent.request.root_id or parent.request.id
                if not parent_id:
                    parent_id = parent.request.id

                if conf.task_inherit_parent_priority:
                    options.setdefault('priority',
                                       parent.request.delivery_info.get('priority'))

        message = amqp.create_task_message(
            task_id, name, args, kwargs, None, None, group_id, group_index,
            expires, retries, chord,
            maybe_list(link), maybe_list(link_error),
            reply_to or self.thread_oid, time_limit, soft_time_limit,
            self.conf.task_send_sent_event,
            root_id, parent_id, shadow, chain,
            argsrepr=options.get('argsrepr'),
            kwargsrepr=options.get('kwargsrepr'),
            )

        queue = options['queue'].routing_key

        message.properties['delivery_mode'] = 2
        message.properties['priority'] = 0
        message.properties['body_encoding'] = 'base64'
        message.properties['delivery_tag'] = uuid()
        message.properties['delivery_info'] = {'exchange': '',
                                               'routing_key': queue}


        payload = json.dumps({
            'body':base64.b64encode(json.dumps(message.body).encode()).decode(),
            'headers':message.headers,
            'properties': message.properties,
            'content-type': 'application/json',
            'content-encoding': 'utf-8',
        })

        client = self.pool.connection.manager.channel.client
        client.hset(f"data:{queue}", task_id, payload)
        client.set(f"{queue}:{task_id}", "", ex=countdown)

