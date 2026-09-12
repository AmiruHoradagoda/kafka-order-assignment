from exceptions import PermanentProcessingError


def validate_order(order):

    if not order["orderId"]:
        raise PermanentProcessingError(
            "Order ID cannot be empty"
        )

    if not order["product"]:
        raise PermanentProcessingError(
            "Product cannot be empty"
        )

    if order["price"] <= 0:
        raise PermanentProcessingError(
            "Order price must be greater than zero"
        )