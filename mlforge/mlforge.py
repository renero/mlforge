"""

Pipeline class to define and run several execution steps.
@author: Jesús Renero

"""

# pylint: disable=E1101:no-member, W0201:attribute-defined-outside-init, W0511:fixme
# pylint: disable=C0103:invalid-name, R0902:too-many-instance-attributes
# pylint: disable=C0116:missing-function-docstring, C0115:missing-class-docstring
# pylint: disable=R0913:too-many-arguments, R0903:too-few-public-methods
# pylint: disable=R0914:too-many-locals, R0915:too-many-statements
# pylint: disable=W0106:expression-not-assigned, R1702:too-many-branches
# pylint: disable=W0212:protected-access

import importlib
import io
import inspect
import logging
import time
import types
import typing
from dataclasses import asdict, dataclass
from importlib import import_module
from random import getrandbits
from textwrap import indent
from typing import Any, Callable, List, Union

import yaml
from rich import print as rp
from rich.columns import Columns
from rich.console import Console
from rich.pretty import Pretty
from rich.table import Table

from mlforge.logconfig import LogConfig
from mlforge.progbar import ProgBar


@dataclass
class Stage:
    _num: int = None
    _id: str = None
    attribute_name: str = None
    method_name: str = None
    _method_call: callable = None
    class_name: type = None
    _parameters: dict = None
    arguments: dict = None
    _timestamp_start: float = None
    _timestamp_end: float = None
    _duration: float = None


class Pipeline:
    """
    Pipeline class allows to define several execution steps to run sequentially.
    Pipeline is initialized with a host object that contains the parameters to be
    used in the execution steps.
    At each step, the pipeline can call either a function or a class. If a class
    is called, the pipeline will call the default method of the class. Such a default
    method must be specified in the pipeline constructor.
    If a function is called, it must be present globally or inside the host object.
    The pipeline can also create an attribute inside the host object with the value
    returned by the function or the fit method of the class.

    Parameters
    ----------
    host: object
        Object containing the parameters to be used in the execution steps.
    log_name: str
        Name of the logger.
    log_level: str
        Level of the logger.
    log_fname: str
        Name of the log file.
    prog_bar: bool
        Flag indicating whether to display the progress bar.
    description: str
        Description of the pipeline to be displayed by the progbar.
    subtask: bool
        Indicates whether to show a secondary progress bar to show the progress of
        each stage. This flag requires that each method in the pipeline will call
        the `ProgBar` object to update the progress bar.
    verbose: bool
        Flag indicating whether to display verbose output.
    silent: bool
        Flag indicating whether to disable the progress bar.
    """

    def __init__(
            self,
            host: type = None,
            log_name: str = None,
            log_level: str = "info",
            log_fname: str = None,
            prog_bar: bool = True,
            description: str = None,
            subtask: bool = False,
            verbose: bool = False,
            silent: bool = False):

        # First thing is knowing who is calling the pipeline.
        caller = inspect.stack()[1]
        self.caller_module = caller.frame.f_globals['__name__']
        self.caller_filename = caller[0].f_code.co_filename
        self.caller_filename = self.caller_filename.split('/')[-1]

        self.host = host
        self.pipeline = []
        self.verbose = verbose
        self.prog_bar = prog_bar
        self.description = description
        self.subtask = subtask
        self.silent = silent
        self.attributes_ = {}
        self.objects_ = {'host': self.host}
        self.pbar = None
        self.run_ = False

        # Rules to sort out what to display
        if silent:
            self.verbose = False
            self.prog_bar = False
            self.subtask = False
        elif verbose:
            self.prog_bar = False
            self.subtask = False
        elif prog_bar:
            self.verbose = False

        # Set logging.
        self.logger = LogConfig.setup_logging(
            name=log_name, level=log_level, fname=log_fname,
            caller_filename=self.caller_filename)
        self.logger.debug('Pipeline initialized')

    def close(self):
        """
        Close the pipeline.
        """
        self._pbar_close()
        self.logger.debug('Pipeline closed')
        self.logger = None
        logging.shutdown()

    def from_list(self, steps: list):
        """
        Load a pipeline from a list of steps.

        Parameters
        ----------
        steps: list
            List of steps to be run. Each step can be a tuple containing the name
            of the attribute to be created in the host object and the function or
            class to be called. But also, each step can be a function or method name.
            In the case of a tuple, the value returned by the function or the fit
            method of the class will be assigned to the attribute of the host object.
            In the case of a function or method name, the value returned by the
            function or the fit method of the class will not be
            assigned to any attribute of the host object.

        """
        # Assert steps list is not empty
        assert steps, "List of steps is empty. No steps to run."

        self._m(
            f"Into '{self.from_list.__name__}' with '{len(steps)}' steps")
        for step_number, step_name in enumerate(steps):
            # Create a new stage of type Stage, and initialize it with the step number
            # and a random id.
            stage = Stage(
                step_number, f"{getrandbits(32):08x}",
                None, None, None, None, None, None)

            self._m(f"> Step #{step_number}({stage._id}) {str(step_name)}")

            # Get the method to be called, the parameters that the
            # method accepts and the arguments to be passed to the method.
            # The variable name is the name to be given to the result of the call.
            # vble_name, step_call, step_parameters, step_arguments = \
            #     self._get_step_components(step_name, stage)
            stage = self._get_step_components(step_name, stage)

            self.pipeline.append(stage)

    def from_config(self, config_filename: str):
        """
        Load a pipeline from a YAML configuration file.

        Parameters
        ----------
        config_filename: str
            Name of the YAML configuration file.
        """
        self._m(f"Into '{self.from_config.__name__}' with "
                f"config_filename='{config_filename}'")

        with open(config_filename, 'r', encoding='utf-8') as file:
            config = yaml.safe_load(file)

        # Retrieve the caller's module name
        caller_module = inspect.stack()[1].frame.f_globals['__name__']

        # Process the config and set the pipeline steps
        self.pipeline = self._process_config(config, caller_module)

    def add_stages(self, stages: list):
        """
        Add stages to the pipeline.

        Parameters
        ----------
        stages: list
            List of stages to be added to the pipeline.
        """
        self._m(
            f"Into '{self.add_stages.__name__}' with '{len(stages)}' stages")

        # This method can be called wiht a pipeline that already has stages.
        last_idx = len(self.pipeline)

        for idx, stage in enumerate(stages):
            stage._num = idx + last_idx
            stage._id = f"{getrandbits(32):08x}"
            self.pipeline.append(stage)

    def run(self, num_steps: int = None):
        """
        Run the pipeline.

        Parameters
        ----------
        num_steps: int
            The number of steps that contains the pipeline. If not specified, the
            number of steps in the pipeline is used.
        """
        assert self.pipeline, "Pipeline is empty. No steps to run."
        n_steps = len(self.pipeline) if num_steps is None else num_steps
        self._pbar_create(name=self.description, num_steps=n_steps)
        self._m(f"RUN pipeline with {len(self.pipeline)} steps")

        self.logger.info('Pipeline execution started')
        for stage_nr, stage in enumerate(self.pipeline):
            self._m(f"  > Step #{stage._num:>03d}({stage._id})\n"
                    f"    > attribute_name: {stage.attribute_name}\n"
                    f"    > method_name: {stage.method_name}\n"
                    f"    > class_name: {stage.class_name}\n"
                    f"    > arguments: {self._pretty_as_text(stage.arguments, '      ')}", end="")
            # Check if step_name is a method within Host, or in globals
            stage._method_call = self._get_callable_method(
                stage.method_name, stage.class_name)
            stage._parameters = self._get_method_signature(stage._method_call)

            # If step_parameters has 'self' as first key, remove it.
            if 'self' in stage._parameters.keys():
                stage._parameters.pop('self')

            # Given the parameters that the method accepts and the arguments
            # passed for the method, build the parameters to be passed to the
            # method, using default values or values from the host object.
            step_parameters = self._build_params(
                stage._parameters, stage.arguments)

            self.logger.info("Running step #%03d(%s) started",
                             stage._num, stage._id)
            stage._timestamp_start = time.time()
            return_value = self._run_step(stage._method_call, step_parameters)
            stage._timestamp_end = time.time()
            stage._duration = stage._timestamp_end - stage._timestamp_start
            self.logger.info("Running step #%03d(%s) finished",
                             stage._num, stage._id)

            # If return value needs to be stored in a variable, do it.
            if stage.attribute_name is not None:
                # If host is None, I assign the return value to the global variable
                if self.host is None:
                    self.attributes_[stage.attribute_name] = return_value
                    # globals()[stage.attribute_name] = return_value
                else:
                    setattr(self.host, stage.attribute_name, return_value)
                # Check if the new attribute created is an object and if so,
                # add it to the list of objects.
                if not isinstance(return_value, type):
                    self.objects_[stage.attribute_name] = return_value
                self._m(f"      New attribute: <{stage.attribute_name}>")

            print("-"*100) if self.verbose else None
            self._pbar_update(self.description, stage_nr + 1)

        self._pbar_close()
        ProgBar.clear()
        self.logger.info('Pipeline execution finished')
        self.run_ = True

    def _get_step_components(self, forge_step: tuple, stage: Stage):
        """
        Get the components of a forge step, in a way that can be used to invoke it.

        Parameters
        ----------
        forge_step: tuple
            Tuple containing the components of a forge step.
        stage: Stage
            Named tuple to store the parameters of each step.

        Returns
        -------
        attribute_name: str
            Name of the attribute to be created in the host object.
        """
        # Assert that forge_step is a unique value (either a str or class_name)
        # or a tuple that is not empty.
        assert forge_step is not None, "Forge step cannot be None"
        assert isinstance(forge_step, (str, type, tuple)), \
            f"Forge step '{forge_step}' must be a string, class name or a tuple"
        assert len(forge_step) > 0, "Forge step cannot be an empty tuple"

        self._m(f"  > Into '{self._get_step_components.__name__}' "
                f"with forge_step='{forge_step}'")

        stage.attribute_name, stage.method_name, stage.class_name, stage.arguments = \
            self._parse_step(forge_step)

        return stage

    def _get_method_signature(self, method_call: Callable):
        """
        Get the signature of a method.

        Parameters:
        -----------
        method_call: Callable
            The method to get the signature of.

        Returns:
        --------
        method_parameters: dict
            A dictionary containing the method's parameters and their default values.
        """
        parameters = inspect.signature(method_call).parameters
        if parameters is None:
            return None

        return {arg: parameters[arg].default for arg in parameters.keys()}

    def _parse_step(self, forge_step):
        """
        Parse a forge step to know who is who. The options are

        'method_name'
        ClassHolder
        ('method_name')
        (ClassHolder)

        ('method_name', ClassHolder)
        ('new_attribute', 'method_name')
        ('new_attribute', ClassHolder)
        ('method_name', {'param1': 'value1'})

        ('new_attribute', 'method_name', ClassHolder)
        ('new_attribute', 'method_name', {'param1': 'value1'})
        ('new_attribute', ClassHolder, {'param1': 'value1'})
        ('method_name', ClassHolder, {'param1': 'value1'})

        ('new_attribute', 'method_name', ClassHolder, {'param1': 'value1'})
        ('new_attribute', 'method_name', 'object_name', {'param1': 'value1'})

        Parameters
        ----------
        step_name: str
            The forget step.

        Returns
        -------
        list
            A 4-tuple with the attribute name, the method name, the class name
            and the parameters.
        """
        self._m(f"    > Into '{self._parse_step.__name__}' "
                f"with forge_step='{forge_step}'")

        attribute_name, method_name, class_name, parameters = None, None, None, None
        if not isinstance(forge_step, (tuple)):
            forge_step = (forge_step,)

        # Assert that the length of the tuple is between 1 and 4
        assert len(forge_step) > 0 and len(forge_step) < 5, \
            f"Tuple '{forge_step}' must have between 1 and 4 elements"

        # Checks---------------------------------------------------------------
        # 'method_name'
        # ClassHolder
        # ('method_name')
        # (ClassHolder)
        if len(forge_step) == 1:
            if isinstance(forge_step[0], str):
                method_name = forge_step[0]
            elif isinstance(forge_step[0], type):
                class_name = forge_step[0]
            else:
                raise ValueError(
                    f"Parameter \'{forge_step}\' must be a string or a class")
        # Checks---------------------------------------------------------------
        # ('method_name', ClassHolder)
        # ('new_attribute', 'method_name')
        # ('new_attribute', ClassHolder)
        # ('method_name', {'param1': 'value1'})
        elif len(forge_step) == 2:
            if isinstance(forge_step[1], type):
                # Check if the first element is a method name or an attribute name
                if isinstance(forge_step[0], str) and \
                        self._get_callable_method(
                            forge_step[0], forge_step[1]) is not None:
                    method_name, class_name = forge_step
                elif isinstance(forge_step[0], str) and \
                        self._get_callable_method(forge_step[0], forge_step[1]) is None:
                    attribute_name, class_name = forge_step
                else:
                    raise ValueError(
                        f"First element of tuple \'{forge_step}\' must be a string "
                        f"or a method name, when the second element is a class")
            elif isinstance(forge_step[1], dict) and isinstance(forge_step[0], str):
                method_name, parameters = forge_step
            elif isinstance(forge_step[0], str) and isinstance(forge_step[1], str):
                attribute_name, method_name = forge_step
            else:
                raise ValueError(
                    f"Tuple \'{forge_step}\' with 2 elements must be either "
                    f"(str, class), (str, str) or (str, dict)")
        # Checks---------------------------------------------------------------
        # ('new_attribute', 'method_name',  ClassHolder)
        # ('new_attribute', 'method_name', {'param1': 'value1'})
        # ('new_attribute',  ClassHolder,  {'param1': 'value1'})
        # ('method_name',    ClassHolder,  {'param1': 'value1'})
        elif len(forge_step) == 3:
            if isinstance(forge_step[0], str) and isinstance(forge_step[1], str):
                if isinstance(forge_step[2], type):
                    attribute_name, method_name, class_name = forge_step
                elif isinstance(forge_step[2], dict):
                    attribute_name, method_name, parameters = forge_step
                else:
                    raise ValueError(
                        f"Tuple \'{forge_step}\' with 3 elements must be "
                        f"(str, str, class) or (str, str, dict)")
            elif self._get_callable_method(forge_step[0], forge_step[1]) is None and \
                    isinstance(forge_step[1], type):
                attribute_name, class_name, parameters = forge_step
            elif self._get_callable_method(forge_step[0], forge_step[1]) is not None \
                    and isinstance(forge_step[1], type):
                method_name, class_name, parameters = forge_step
            else:
                raise ValueError(
                    f"Tuple \'{forge_step}\' with 3 elements must be either "
                    f"(str, str, dict) or (str, class, dict)")
        # Checks---------------------------------------------------------------
        # ('new_attribute', 'method_name', ClassHolder, {'param1': 'value1'})
        # ('new_attribute', 'method_name', 'object_name', {'param1': 'value1'})
        elif len(forge_step) == 4:
            if isinstance(forge_step[1], str) and isinstance(forge_step[2], type) and \
                    isinstance(forge_step[3], dict):
                attribute_name, method_name, class_name, parameters = forge_step
            else:
                raise ValueError(
                    f"Tuple \'{forge_step}\' with 4 elements must be "
                    f"(str, str, class, dict)")

        return (attribute_name, method_name, class_name, parameters)

    def _get_callable_method(
            self,
            method_name: str,
            class_name: type = None) -> callable:
        """
        Given a method name, get the callable method from the `host` object, this
        very object, or the globals. If the method is not found, return None.

        Parameters
        ----------
        method_name: str
            Name of the method to be called.

        Returns
        -------
        method: callable
            Method to be called, or None if the method is not found.
        """
        self._m(f"      > Into '{self._get_callable_method.__name__}' with "
                f"method_name='{method_name}', class_name='{class_name}'")

        # Assert that method_name is a string or None
        assert method_name is None or \
            isinstance(method_name, str), \
            f"Method name '{method_name}' must be a string or None"

        # If class_name is not None, check if method is a method of the class.
        if class_name is not None and inspect.isclass(class_name):
            if method_name is not None:
                if hasattr(class_name, method_name):
                    return getattr(class_name, method_name)
            else:
                module_name = class_name.__module__
                module = import_module(module_name)
                return getattr(module, class_name.__name__)
            return None

        # Check if the class is a valid class
        if class_name is not None and not inspect.isclass(class_name):
            raise AttributeError(f"Parameter '{class_name}' must be a class")

        # Check if 'method_name' is a method of the host object.
        if hasattr(self.host, method_name):
            return getattr(self.host, method_name)

        # Check if 'method_name' is a method in the pipeline object.
        if hasattr(self, method_name):
            return getattr(self, method_name)

        # Check if 'method_name' is a function in globals.
        if method_name in globals():
            return globals()[method_name]

        # Check if 'method_name' contains a dot (.) and if so, try to get the
        # method from the object after the dot.
        if '.' in method_name:
            obj_name, method_name = method_name.split('.')
            if hasattr(self.host, obj_name):
                obj = getattr(self.host, obj_name)
                return getattr(obj, method_name)
            elif obj_name in self.objects_:
                obj = self.objects_[obj_name]
                return getattr(obj, method_name)
            raise ValueError(
                f"Object {obj_name} not found in host object")

        return None

    def _build_params(self, method_parameters, method_arguments) -> dict:
        """
        This method builds the parameters to be passed to the method, using default
        values or values from the host object.

        Parameters
        ----------
        method_parameters: dict
            Dictionary containing the parameters that the method accepts.
        method_arguments: dict
            Dictionary containing the arguments to be passed to the method.

        Returns
        -------
        params: dict
            Dictionary containing the parameters to be passed to the method.

        """
        self._m(
            f"        > Into '{self._build_params.__name__}' with "
            f"method_parameters='\n{self._pretty_as_text(method_parameters, "          > ")}"
            f"          and method_arguments='{method_arguments}'")

        params = {}
        for parameter, default_value in method_parameters.items():
            if method_arguments is not None:
                # If the parameter is in method_arguments, use the value from
                # method_arguments.
                if parameter in method_arguments:
                    # Two possibilities here: either the parameter is a normal value,
                    # in which case we simply take it, or is the name of an object
                    # created in a previous step, in which case we take the object.
                    # But first, check if the parameter is hashable.
                    if not isinstance(method_arguments[parameter], typing.Hashable):
                        params[parameter] = method_arguments[parameter]
                        continue
                    if method_arguments[parameter] in self.objects_:
                        params[parameter] = self.objects_[
                            method_arguments[parameter]]
                    # XXX experimental
                    elif isinstance(method_arguments[parameter], str):
                        if hasattr(self.host, method_arguments[parameter]):
                            params[parameter] = getattr(
                                self.host, method_arguments[parameter])
                        else:  #  It's a literal string
                            params[parameter] = method_arguments[parameter]
                    # XXX experimental
                    else:
                        params[parameter] = method_arguments[parameter]
                    continue

            # But always, try to get the parameter from the host object or globals.
            if hasattr(self.host, parameter):
                params[parameter] = getattr(self.host, parameter)
            elif parameter in globals():
                params[parameter] = globals()[parameter]
            # or if the parameter has a default value, use it.
            elif default_value is not inspect.Parameter.empty:
                params[parameter] = default_value
                # continue
            else:
                raise ValueError(
                    f"Parameter \'{parameter}\' not found in host object or globals")

        # Final step: check that all the arguments suggested have been collected
        # from the method_arguments and set into `params`. Otherwise, I'm specifying
        # arguments that are not present in the method.
        if method_arguments is not None:
            for arg in method_arguments.keys():
                if arg not in params.keys():
                    raise ValueError(
                        f"Parameter \'{arg}\' not found in method_parameters")

        return params

    def _run_step(
            self,
            step_name: Union[Any, str],
            list_of_params: List[Any] = None) -> Any:
        """
        Run a step of the pipeline.

        Parameters
        ----------
        step_name: str
            Function or class to be called.
        list_of_params: list
            List of parameters to be passed to the function or class.

        Returns
        -------
        return_value: any
            Value returned by the function or the fit method of the class.
        """
        return_value = None
        if list_of_params is None:
            list_of_params = []

        # Check if step_name is a function or a class already in globals
        if step_name in globals():
            step_name = globals()[step_name]
            # check if type of step_name is a function
            if isinstance(step_name, (types.FunctionType, types.MethodType)):
                return_value = step_name(**list_of_params)
            # check if type of step_name is a class
            elif isinstance(step_name, type):
                obj = step_name(**list_of_params)
                obj.fit()
                return_value = obj
            else:
                raise TypeError("step_name must be a class or a function")
        # Check if step_name is a function or a class in the calling module
        elif not isinstance(step_name, str) and hasattr(step_name, '__module__'):
            # check if type of step_name is a function
            if isinstance(step_name, (types.FunctionType, types.MethodType)):
                return_value = step_name(**list_of_params)
            # check if type of step_name is a class
            elif isinstance(step_name, type) or inspect.isclass(step_name):
                obj = step_name(**list_of_params)
                return_value = obj
            else:
                raise TypeError("step_name must be a class or a function")
        # Check if step_name is a method of the host object
        elif hasattr(self.host, step_name):
            step_name = getattr(self.host, step_name)
            # check if type of step_name is a function
            if isinstance(step_name, (types.FunctionType, types.MethodType)):
                return_value = step_name(**list_of_params)
            else:
                raise TypeError(
                    "step_name inside host object must be a function")
        # Consider that step_name is a method of some of the intermediate objects
        # in the pipeline
        else:
            # check if step name is of the form object.method
            if '.' not in step_name:
                raise ValueError(
                    f"step_name ({step_name}) must be object's method: object.method")
            method_call = step_name
            root_object = self.host
            while '.' in method_call:
                call_composition = step_name.split('.')
                obj_name = call_composition[0]
                method_name = method_name = '.'.join(call_composition[1:])
                obj = getattr(root_object, obj_name)
                call_name = getattr(obj, method_name)
                method_call = '.'.join(method_name.split('.')[1:])
            return_value = call_name(**list_of_params)

        self._m(f"      > Return value: {type(return_value)}")
        return return_value

    def show(self):
        """
        Show the pipeline. Print cards with the steps and the description of each step.
        """
        columns_layout = []
        table = []
        num_stages = len(self.pipeline)
        for i in range(num_stages):
            table.append(Table())
            table[i].add_column(
                f"[white]Stage #{i}, id: #{self.pipeline[i]._id}[/white]",
                justify="left", no_wrap=True)
            line = ""
            # Loop through the elements of the stage tuple
            for k, v in asdict(self.pipeline[i]).items():
                if k == '_num' or k == '_id' or k == '_method_call' or \
                    k == "_parameters" or k == "_timestamp_start" or \
                        k == "_timestamp_end" or k == "_duration" or v is None:
                    continue
                if isinstance(v, dict) and v:
                    line += f"[yellow1]{k}[/yellow1]:\n"
                    for k1, v1 in v.items():
                        line += f"- [orange1]{k1}[/orange1]: {v1}\n"
                elif isinstance(v, type):
                    line += f"[yellow1]{k}[/yellow1]: {v.__name__}\n"
                else:
                    line += f"[yellow1]{k}[/yellow1]: {v}\n"

            # Remove trailing newline from `line`
            line = line.rstrip("\n")
            table[i].add_row(line)
            columns_layout.append(table[i])
            if i < num_stages-1:
                columns_layout.append("\n->")

        columns = Columns(columns_layout)
        rp(columns)

    def duration(self):
        """
        This method displays the duration of each stage of the pipeline, and the
        total duration of the pipeline. Each stage duration is displayed in seconds
        or in milliseconds, depending on the duration. Each line represents a stage
        and the last line represents the total duration of the pipeline.
        """
        print("Duration of each stage of the pipeline:")
        for stage in self.pipeline:
            if stage._duration < 1:
                print(
                    f"Stage #{stage._num:>03d} ({stage._id}): "
                    f"{stage._duration*1000:.03f} ms")
            else:
                print(
                    f"Stage #{stage._num:>03d} ({stage._id}): "
                    f"{stage._duration:.2f} s")

        total_duration = sum([stage._duration for stage in self.pipeline])
        if total_duration < 1:
            print(
                f"Total duration: {total_duration*1000:.03f} ms")
        else:
            print(
                f"Total duration: {total_duration:.2f} s")

    def len(self):
        """
        Return the number of steps in the pipeline.
        """
        return len(self.pipeline)

    def _process_config(self, config: dict, caller_module) -> dict:
        """
        Process the YAML configuration and convert it into a list of pipeline steps.

        Parameters
        ----------
        config: dict
            Dictionary containing the YAML configuration.

        Returns
        -------
        steps: list
            List of pipeline steps.
        """
        steps = []

        self._m(f"> caller_module: {caller_module}")
        module = importlib.import_module(caller_module)

        for nr, (step_id, step_contents) in enumerate(config.items()):
            stage = Stage()
            stage._num = nr
            stage._id = step_id
            for k, v in step_contents.items():
                if k == 'attribute':
                    stage.attribute_name = v
                elif k == 'method':
                    stage.method_name = v
                elif k == 'class':
                    try:
                        stage.class_name = getattr(module, v)
                    except AttributeError as exc:
                        raise AttributeError(
                            f"Class '{v}' not found in module '{module}'") from exc
                elif k == 'arguments':
                    stage.arguments = v
                else:
                    raise ValueError(
                        f"Key '{k}' not recognized in the configuration")
            steps.append(stage)

        self._m(f"> Processed {len(steps)} steps")
        return steps

    def get_attribute(self, attribute_name):
        """
        This method returns the value of the attribute with the given name. If the
        pipeline has not been run, the attribute will not exist and the method will
        return None. If the attribute does not exist, the method will raise an
        AttributeError.
        """
        if not self.run_:
            return None
        if attribute_name in self.attributes_:
            return self.attributes_[attribute_name]
        raise AttributeError(f"Attribute '{attribute_name}' not found")

    def _pbar_create(self, name: str, num_steps: int) -> ProgBar:
        """
            Creates a progress bar using the tqdm library.

            The progress bar is used to track the progress of the pipeline execution.

            Paramaeters:
            ------------
            name (str): The name of the progress bar. Default is None.
            num_steps (int): The number of steps in the pipeline. Default is 0.

            Returns:
                A ProgBar object.
        """
        if len(self.pipeline) == 0 or self.silent or not self.prog_bar:
            return None
        self.pbar = ProgBar(
            name=name, num_steps=num_steps, verbose=self.verbose)

        return self.pbar

    def _pbar_update(self, name, step=1):
        """
        Update the progress bar by the specified step.

        Parameters:
        - step (int): The step size to update the progress bar. Default is 1.
        """
        if self.pbar is None:
            return
        if name is None:
            name = "subtask_0"
        self.pbar.update_subtask(name, step)

    def _pbar_close(self):
        """
        Close the progress bar.
        """
        if self.pbar is None:
            return
        self.pbar.remove(self.description)

    def _m(self, m: str, end="\n"):
        """
        Printout message if verbose is set to True, and log.debug the message.
        """
        if self.verbose:
            print(m, end=end)
        m = m.replace('  ', '')
        m = m.replace('> ', '')
        # Remove any newline character from the message
        m = m.replace('\n', '')
        self.logger.debug(m)

    def _pretty_as_text(self, obj, prefix="") -> str:
        """
        This method returns a rich pretty-printed string representation of an object.
        Parameters
        ----------
        obj: Any
            The object to be pretty-printed.
        prefix: str
            The prefix to be added to each line of the pretty-printed string.

        Returns
        -------
        pretty_str: str
            The pretty-printed string representation of the object.
        """
        console = Console(record=True, width=100, file=io.StringIO())
        console.print(Pretty(obj, expand_all=True))
        return indent(console.export_text(styles=True), prefix)

    def contains_method(self, method_name: str, exact_match: bool = False) -> bool:
        """
        This method checks if the given method is part of the pipeline.

        Parameters
        ----------
        method_name: str
            The name of the method to check.
        exact_match: bool
            If True, the method name must match exactly. If False, the method name
            can be any substring of the method name.

        Returns
        -------
        stage: int
            The number of times that the method is found.
        """
        if method_name is None:
            raise ValueError("method_name must not be None")
        if self.pipeline is None:
            raise ValueError("pipeline must not be None")

        if not isinstance(method_name, str):
            raise TypeError("method_name must be a string.")
        if not isinstance(exact_match, bool):
            raise TypeError("exact_match must be a boolean.")

        num_found = 0
        if exact_match:
            for stage in self.pipeline:
                if stage.method_name == method_name:
                    num_found += 1
        else:
            for stage in self.pipeline:
                if stage.method_name is None:
                    continue
                if method_name in stage.method_name:
                    num_found += 1

        return num_found

    def contains_class(self, class_name: str) -> bool:
        """
        This method checks if the given class is part of the pipeline.

        Parameters
        ----------
        class_name: str
            The name of the class to check.

        Returns
        -------
        stage: Stage
            The stage that contains the class.
        """
        if class_name is None:
            raise ValueError("class_name must not be None")
        if self.pipeline is None:
            raise ValueError("pipeline must not be None")

        if not isinstance(class_name, str):
            raise TypeError("class_name must be a string.")

        return any(stage.class_name == class_name for stage in self.pipeline)

    def contains_argument(self, attribute_name: str) -> bool:
        """
        This method checks if the given attribute is part of the pipeline.

        Parameters
        ----------
        attribute_name: str
            The name of the attribute to check.

        Returns
        -------
        stage: Stage
            The number of times that the argument is found.
        """
        if attribute_name is None:
            raise ValueError("attribute_name must not be None")
        if self.pipeline is None:
            raise ValueError("pipeline must not be None")

        if not isinstance(attribute_name, str):
            raise TypeError("attribute_name must be a string.")

        # Go through every stage in the pipeline, and then through every argument,
        # if any, to check if the attribute_name matches
        num_found = 0
        for stage in self.pipeline:
            # Check if there's an argument
            if stage.arguments is not None:
                if stage.arguments.get(attribute_name) is not None:
                    num_found += 1

        return num_found

    def get_argument_value(self, attribute_name: str):
        """
        This method returns the value of the given attribute.

        Parameters
        ----------
        attribute_name: str
            The name of the attribute to get the value of.

        Returns
        -------
        attribute_value: Any
            The value of the attribute.
        """
        if attribute_name is None:
            raise ValueError("attribute_name must not be None")
        if self.pipeline is None:
            raise ValueError("pipeline must not be None")

        if not isinstance(attribute_name, str):
            raise TypeError("attribute_name must be a string.")

        # Go through every stage in the pipeline, and then through every argument,
        # if any, to check if the attribute_name matches
        for stage in self.pipeline:
            # Check if there's an argument
            if stage.arguments is not None:
                if stage.arguments.get(attribute_name) is not None:
                    return stage.arguments[attribute_name]

        return None

    def all_argument_values(self, attribute_name: str):
        """
        This method returns a list with all values of the given attribute
        in the pipeline. If the attribute is not found, the method returns None.

        Parameters
        ----------
        attribute_name: str
            The name of the attribute to get the value of.

        Returns
        -------
        attribute_values: list
            A list with all values of the attribute.
        """
        assert attribute_name is not None, "attribute_name must not be None"
        assert self.pipeline != [], "pipeline must be initialized"
        assert isinstance(
            attribute_name, str), "attribute_name must be a string"

        attribute_values = []
        for stage in self.pipeline:
            if stage.arguments is not None:
                if stage.arguments.get(attribute_name) is not None:
                    attribute_values.append(stage.arguments[attribute_name])

        return attribute_values
