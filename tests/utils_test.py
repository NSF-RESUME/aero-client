from aero_client.utils import aero_analysis


def test_analysis_decorator():
    def user_function(input_data1, input_data2, arg1, arg2):
        assert input_data1 == {"id": "uuid1", "version": 10}
        assert input_data2 == {"id": "uuid2", "version": 2}
        assert arg1 == ["apples", "bananas", "pears"]
        assert arg2 == 9999

        return [
            {"name": "output_data1", "checksum": "1234", "size": 2},
            {"name": "output_data2", "checksum": "2222", "size": 3},
        ]

    task_kwargs = {
        "aero": {
            "input_data": {
                "input_data1": {"id": "uuid1", "version": 10},
                "input_data2": {"id": "uuid2", "version": 2},
            },
            "output_data": {
                "output_data1": {"id": "3456"},
                "output_data2": {"id": "5678"},
            },
        },
        "function_args": {"arg1": ["apples", "bananas", "pears"], "arg2": 9999},
        "flow_id": "flow10",
    }

    output_kwargs = aero_analysis(user_function)(**task_kwargs)
    assert output_kwargs["aero"]["output_data"]["output_data1"] == {
        "id": "3456",
        "checksum": "1234",
        "size": 2,
    }

    assert output_kwargs["aero"]["output_data"]["output_data2"] == {
        "id": "5678",
        "checksum": "2222",
        "size": 3,
    }

    assert "flow_id" in output_kwargs.keys()
