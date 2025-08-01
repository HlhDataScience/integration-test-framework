from transactional_abstractions import TransactionalFilterInterface

class UserIdSqlSelector:
    """
    This class interacts with the SQLite database
    to extract ids based on criteria
    to pass into the testing pipeline.
    """
    ...

class UserIdTransactionalFilter(TransactionalFilterInterface):
    """
    This class interacts with the students Ids collected from UserIdSqlSelector.
    It uses them to further filter and obtain information useful for testing.
    It inherits from TransactionalFilterInterface, implementing all its methods.
    """
    ...