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
        self.CreateTable = channel.stream_unary(
                '/db.Db/CreateTable',
                request_serializer=proto_dot_db__pb2.SourceIpc.SerializeToString,
                response_deserializer=proto_dot_db__pb2.Message.FromString,
                )
        self.InsertTable = channel.stream_unary(
                '/db.Db/InsertTable',
                request_serializer=proto_dot_db__pb2.SourceIpc.SerializeToString,
                response_deserializer=proto_dot_db__pb2.Message.FromString,
                )
        self.UpsertTable = channel.stream_unary(
                '/db.Db/UpsertTable',
                request_serializer=proto_dot_db__pb2.SourceIpc.SerializeToString,
                response_deserializer=proto_dot_db__pb2.Message.FromString,
                )
        self.MaterializeTable = channel.unary_unary(
                '/db.Db/MaterializeTable',
                request_serializer=proto_dot_db__pb2.Table.SerializeToString,
                response_deserializer=proto_dot_db__pb2.Message.FromString,
                )
        self.GetTableInfo = channel.unary_unary(
                '/db.Db/GetTableInfo',
                request_serializer=proto_dot_db__pb2.Table.SerializeToString,
                response_deserializer=proto_dot_db__pb2.TableInfo.FromString,
                )
        self.SelectIpc = channel.stream_stream(
                '/db.Db/SelectIpc',
                request_serializer=proto_dot_db__pb2.Sql.SerializeToString,
                response_deserializer=proto_dot_db__pb2.SqlResults.FromString,
                )


class DbServicer(object):
    """Interface exported by the server.
    """

    def CreateTable(self, request_iterator, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def InsertTable(self, request_iterator, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def UpsertTable(self, request_iterator, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def MaterializeTable(self, request, context):
        """Table based handling
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def GetTableInfo(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def SelectIpc(self, request_iterator, context):
        """SwapPartition
        DeletePartition

        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_DbServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'CreateTable': grpc.stream_unary_rpc_method_handler(
                    servicer.CreateTable,
                    request_deserializer=proto_dot_db__pb2.SourceIpc.FromString,
                    response_serializer=proto_dot_db__pb2.Message.SerializeToString,
            ),
            'InsertTable': grpc.stream_unary_rpc_method_handler(
                    servicer.InsertTable,
                    request_deserializer=proto_dot_db__pb2.SourceIpc.FromString,
                    response_serializer=proto_dot_db__pb2.Message.SerializeToString,
            ),
            'UpsertTable': grpc.stream_unary_rpc_method_handler(
                    servicer.UpsertTable,
                    request_deserializer=proto_dot_db__pb2.SourceIpc.FromString,
                    response_serializer=proto_dot_db__pb2.Message.SerializeToString,
            ),
            'MaterializeTable': grpc.unary_unary_rpc_method_handler(
                    servicer.MaterializeTable,
                    request_deserializer=proto_dot_db__pb2.Table.FromString,
                    response_serializer=proto_dot_db__pb2.Message.SerializeToString,
            ),
            'GetTableInfo': grpc.unary_unary_rpc_method_handler(
                    servicer.GetTableInfo,
                    request_deserializer=proto_dot_db__pb2.Table.FromString,
                    response_serializer=proto_dot_db__pb2.TableInfo.SerializeToString,
            ),
            'SelectIpc': grpc.stream_stream_rpc_method_handler(
                    servicer.SelectIpc,
                    request_deserializer=proto_dot_db__pb2.Sql.FromString,
                    response_serializer=proto_dot_db__pb2.SqlResults.SerializeToString,
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
    def CreateTable(request_iterator,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.stream_unary(request_iterator, target, '/db.Db/CreateTable',
            proto_dot_db__pb2.SourceIpc.SerializeToString,
            proto_dot_db__pb2.Message.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def InsertTable(request_iterator,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.stream_unary(request_iterator, target, '/db.Db/InsertTable',
            proto_dot_db__pb2.SourceIpc.SerializeToString,
            proto_dot_db__pb2.Message.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def UpsertTable(request_iterator,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.stream_unary(request_iterator, target, '/db.Db/UpsertTable',
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
    def GetTableInfo(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/db.Db/GetTableInfo',
            proto_dot_db__pb2.Table.SerializeToString,
            proto_dot_db__pb2.TableInfo.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def SelectIpc(request_iterator,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.stream_stream(request_iterator, target, '/db.Db/SelectIpc',
            proto_dot_db__pb2.Sql.SerializeToString,
            proto_dot_db__pb2.SqlResults.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)
