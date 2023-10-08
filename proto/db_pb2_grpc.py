# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc

from proto import db_pb2 as proto_dot_db__pb2


class DbStub(object):
    """Interface exported by the server.
    """

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.CreateTable = channel.unary_unary(
                '/db.Db/CreateTable',
                request_serializer=proto_dot_db__pb2.SourceIpc.SerializeToString,
                response_deserializer=proto_dot_db__pb2.Message.FromString,
                )
        self.InsertTable = channel.unary_unary(
                '/db.Db/InsertTable',
                request_serializer=proto_dot_db__pb2.SourceIpc.SerializeToString,
                response_deserializer=proto_dot_db__pb2.Message.FromString,
                )
        self.UpsertTable = channel.unary_unary(
                '/db.Db/UpsertTable',
                request_serializer=proto_dot_db__pb2.SourceIpc.SerializeToString,
                response_deserializer=proto_dot_db__pb2.Message.FromString,
                )
        self.MaterializeTable = channel.unary_unary(
                '/db.Db/MaterializeTable',
                request_serializer=proto_dot_db__pb2.Table.SerializeToString,
                response_deserializer=proto_dot_db__pb2.Message.FromString,
                )
        self.CreateTableStream = channel.stream_unary(
                '/db.Db/CreateTableStream',
                request_serializer=proto_dot_db__pb2.SourceIpc.SerializeToString,
                response_deserializer=proto_dot_db__pb2.Message.FromString,
                )
        self.InsertTableStream = channel.stream_unary(
                '/db.Db/InsertTableStream',
                request_serializer=proto_dot_db__pb2.SourceIpc.SerializeToString,
                response_deserializer=proto_dot_db__pb2.Message.FromString,
                )
        self.SelectIpc = channel.unary_unary(
                '/db.Db/SelectIpc',
                request_serializer=proto_dot_db__pb2.Sql.SerializeToString,
                response_deserializer=proto_dot_db__pb2.ResultIpc.FromString,
                )
        self.SelectsIpc = channel.unary_unary(
                '/db.Db/SelectsIpc',
                request_serializer=proto_dot_db__pb2.Sqls.SerializeToString,
                response_deserializer=proto_dot_db__pb2.ResultsIpc.FromString,
                )


class DbServicer(object):
    """Interface exported by the server.
    """

    def CreateTable(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def InsertTable(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def UpsertTable(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def MaterializeTable(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def CreateTableStream(self, request_iterator, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def InsertTableStream(self, request_iterator, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def SelectIpc(self, request, context):
        """SwapPartition
        DeletePartition
        DropTable
        AntiTable

        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def SelectsIpc(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_DbServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'CreateTable': grpc.unary_unary_rpc_method_handler(
                    servicer.CreateTable,
                    request_deserializer=proto_dot_db__pb2.SourceIpc.FromString,
                    response_serializer=proto_dot_db__pb2.Message.SerializeToString,
            ),
            'InsertTable': grpc.unary_unary_rpc_method_handler(
                    servicer.InsertTable,
                    request_deserializer=proto_dot_db__pb2.SourceIpc.FromString,
                    response_serializer=proto_dot_db__pb2.Message.SerializeToString,
            ),
            'UpsertTable': grpc.unary_unary_rpc_method_handler(
                    servicer.UpsertTable,
                    request_deserializer=proto_dot_db__pb2.SourceIpc.FromString,
                    response_serializer=proto_dot_db__pb2.Message.SerializeToString,
            ),
            'MaterializeTable': grpc.unary_unary_rpc_method_handler(
                    servicer.MaterializeTable,
                    request_deserializer=proto_dot_db__pb2.Table.FromString,
                    response_serializer=proto_dot_db__pb2.Message.SerializeToString,
            ),
            'CreateTableStream': grpc.stream_unary_rpc_method_handler(
                    servicer.CreateTableStream,
                    request_deserializer=proto_dot_db__pb2.SourceIpc.FromString,
                    response_serializer=proto_dot_db__pb2.Message.SerializeToString,
            ),
            'InsertTableStream': grpc.stream_unary_rpc_method_handler(
                    servicer.InsertTableStream,
                    request_deserializer=proto_dot_db__pb2.SourceIpc.FromString,
                    response_serializer=proto_dot_db__pb2.Message.SerializeToString,
            ),
            'SelectIpc': grpc.unary_unary_rpc_method_handler(
                    servicer.SelectIpc,
                    request_deserializer=proto_dot_db__pb2.Sql.FromString,
                    response_serializer=proto_dot_db__pb2.ResultIpc.SerializeToString,
            ),
            'SelectsIpc': grpc.unary_unary_rpc_method_handler(
                    servicer.SelectsIpc,
                    request_deserializer=proto_dot_db__pb2.Sqls.FromString,
                    response_serializer=proto_dot_db__pb2.ResultsIpc.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'db.Db', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))


 # This class is part of an EXPERIMENTAL API.
class Db(object):
    """Interface exported by the server.
    """

    @staticmethod
    def CreateTable(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/db.Db/CreateTable',
            proto_dot_db__pb2.SourceIpc.SerializeToString,
            proto_dot_db__pb2.Message.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def InsertTable(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/db.Db/InsertTable',
            proto_dot_db__pb2.SourceIpc.SerializeToString,
            proto_dot_db__pb2.Message.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def UpsertTable(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/db.Db/UpsertTable',
            proto_dot_db__pb2.SourceIpc.SerializeToString,
            proto_dot_db__pb2.Message.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def MaterializeTable(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/db.Db/MaterializeTable',
            proto_dot_db__pb2.Table.SerializeToString,
            proto_dot_db__pb2.Message.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def CreateTableStream(request_iterator,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.stream_unary(request_iterator, target, '/db.Db/CreateTableStream',
            proto_dot_db__pb2.SourceIpc.SerializeToString,
            proto_dot_db__pb2.Message.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def InsertTableStream(request_iterator,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.stream_unary(request_iterator, target, '/db.Db/InsertTableStream',
            proto_dot_db__pb2.SourceIpc.SerializeToString,
            proto_dot_db__pb2.Message.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def SelectIpc(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/db.Db/SelectIpc',
            proto_dot_db__pb2.Sql.SerializeToString,
            proto_dot_db__pb2.ResultIpc.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def SelectsIpc(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/db.Db/SelectsIpc',
            proto_dot_db__pb2.Sqls.SerializeToString,
            proto_dot_db__pb2.ResultsIpc.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)
