import React from 'react';
import { template_dark } from './PlotlyTemplate';
import {
  ChakraProvider,
  InputGroup,
  extendTheme,
  Input,
  InputRightElement,
  Button,
  useColorModeValue,
  Box,
  Text,
  Spinner,
  Grid,
  theme,
  Tabs,
  TabList,
  TabPanels,
  Tab,
  TabPanel,
} from '@chakra-ui/react';
import { ColorModeSwitcher } from './ColorModeSwitcher';
import Plot from 'react-plotly.js';

async function logoutAPI() {
  console.log('logging out');
  return fetch('api/auth/logout', {
    credentials: 'include',
    method: 'GET',
  }).then(response => {
    return response.ok;
  });
}

async function authAPIData(pwd) {
  return fetch('api/auth', {
    credentials: 'include',
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ pw: pwd }),
  }).then(response => {
    return response.ok;
  });
}

async function getAPIData(endpoint) {
  return fetch('api/' + endpoint, {
    credentials: 'include',
    method: 'GET',
  })
    .then(response => {
      if (!response.ok) {
        if (response.status === 403) {
          return response.json();
        }
        throw new Error(
          `Could not fetch data from api. Status: ${response.status}: ${response.statusText}`
        );
      }
      return response.json();
    })
    .then(jsonData => {
      return jsonData;
    });
}

function ReturnPlot(props) {
  const isDarkMode = useColorModeValue(false, true);
  var pf_returns = {
    y: props.return_data['pf_returns'].map(item => item.return),
    x: props.return_data['pf_returns'].map(item => item.date),
    type: 'scatter',
    mode: 'line',
    name: 'Portfolio returns',
    color: 'red',
  };
  var index_returns = {
    y: props.return_data['index_returns'].map(item => item.return),
    x: props.return_data['index_returns'].map(item => item.date),
    type: 'scatter',
    mode: 'line',
    name: 'Index returns',
    color: 'blue',
  };
  return (
    <Plot
      data={[pf_returns, index_returns]}
      layout={{
        title: 'Portfolio vs Index Returns',
        template: isDarkMode ? template_dark : {},
        margin: {
          l: 5,
          r: 5,
        },
        yaxis: {
          tickformat: ',.0%',
          fixedrange: true,
          automargin: true,
        },
        legend: { yanchor: 'top', y: -0.1, x: 1, xanchor: 'right' },
        xaxis: {
          fixedrange: true,
          rangeselector: {
            font: { color: 'black' },
            buttons: [
              {
                count: 1,
                label: '1m',
                step: 'month',
                stepmode: 'backward',
              },
              {
                count: 6,
                label: '6m',
                step: 'month',
                stepmode: 'backward',
              },
              { step: 'all' },
            ],
          },
        },
      }}
      useResizeHandler={true}
      style={{ width: '100%', height: '80%' }}
      config={{ displayModeBar: false }}
    />
  );
}

function PasswordInput(props) {
  const [show, setShow] = React.useState(false);
  const handleClick = () => setShow(!show);
  console.log(props.handleKeyPress);
  console.log(typeof props.handleKeyPress);

  let text = 'Auth Required';
  if ('text' in props) {
    text += props.text;
  }
  return (
    <Box>
      <Text>{text}</Text>
      <InputGroup size="md" maxW="30rem">
        <Input
          pr="4.5rem"
          type={show ? 'text' : 'password'}
          placeholder="Enter password"
          onKeyPress={props.handleKeyPress}
        />
        <InputRightElement width="4.5rem">
          <Button h="1.75rem" size="sm" onClick={handleClick}>
            {show ? 'Hide' : 'Show'}
          </Button>
        </InputRightElement>
      </InputGroup>
    </Box>
  );
}

class Returns extends React.Component {
  render() {
    if (!this.props.loaded) {
      return (
        <Spinner
          thickness="4px"
          speed="0.65s"
          emptyColor="gray.200"
          color="blue.500"
          size="xl"
        />
      );
    }
    if (this.props.auth_required) {
      return (
        <PasswordInput
          handleKeyPress={this.props.handlePwdKeyPress}
          text={this.props.error ? ' | Error: ' + this.props.error_data : ''}
        />
      );
    } else if (this.props.error) {
      return <Text>{this.props.error_data}</Text>;
    }
    return (
      <Box width={['100wv', '100vw', '100vw', '80vw']}>
        <ReturnPlot return_data={this.props.return_data}> </ReturnPlot>
      </Box>
    );
  }
}

function Menu(props) {
  let button = null;
  if (!props.auth_required) {
    button = <Button onClick={props.logout}>Logout</Button>;
  }
  return (
    <Box>
      {button}
      <Tabs align="center">
        <TabList>
          <Tab>Returns</Tab>
          <Tab>Holdings</Tab>
          <Tab>Parameters</Tab>
        </TabList>
        <TabPanels>
          <TabPanel>
            <Returns {...props} />
          </TabPanel>
          <TabPanel>
            <Text> Holdings </Text>
          </TabPanel>
          <TabPanel>
            <Text> Parameters </Text>
          </TabPanel>
        </TabPanels>
      </Tabs>
    </Box>
  );
}

class App extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      return_data: 'Loading',
      loaded: false,
      error: false,
      error_data: 'Unknown error',
      auth_required: false,
    };

    // Don't really understand this yet, but needed to access "this" in functions
    this.logout = this.logout.bind(this);
    this.handlePwdKeyPress = this.handlePwdKeyPress.bind(this);
    this.requestData = this.requestData.bind(this);
  }

  handlePwdKeyPress(e) {
    if (e.key === 'Enter') {
      const pwd = e.target.value;
      authAPIData(pwd).then(success => {
        if (success) {
          this.setState({
            auth_required: false,
            error: false,
            error_data: '',
            loaded: false,
          });
          this.requestData();
        } else {
          console.log('bad pwd');
          this.setState({ error: true, error_data: 'Incorrect password' });
        }
      });
    }
  }

  logout() {
    console.log('logging out');
    logoutAPI();
    console.log('logged out, updated state');
    this.setState({ error: false, auth_required: true });
  }

  requestData() {
    getAPIData('returns')
      .then(jsonData => {
        console.log(jsonData);
        if ('index_returns' in jsonData) {
          console.log('got api data');
          this.setState({ return_data: jsonData, loaded: true });
        } else if (
          'message' in jsonData &&
          jsonData['message'].startsWith('Not authenticated')
        ) {
          console.log('not authenticated');
          this.setState({ loaded: true, auth_required: true });
        }
      })
      .catch(err => {
        console.log(`error ${err}`);
        if (err) {
          this.setState({ error_data: String(err) });
        }
        this.setState({ error: true, loaded: true });
      });
  }

  componentDidMount() {
    this.requestData();
  }

  render() {
    const config = {
      useSystemColorMode: false,
      initialColorMode: 'dark',
    };
    const customTheme = extendTheme({ config });

    return (
      <ChakraProvider theme={customTheme}>
        <Box textAlign="right" fontSize="xl">
          <Grid minH="10vh" p={3}>
            <ColorModeSwitcher justifySelf="flex-end" />
            <Menu
              {...this.state}
              logout={this.logout}
              handlePwdKeyPress={this.handlePwdKeyPress}
            />
            {/* <VStack spacing={8} >
              <Text>
                Edit <Code fontSize="xl">src/App.js</Code> and save to reload.
              </Text>
              <Link
                color="teal.500"
                href="https://chakra-ui.com"
                fontSize="2xl"
                target="_blank"
                rel="noopener noreferrer"
              >
                Learn Chakra
              </Link>
            </VStack> */}
          </Grid>
        </Box>
      </ChakraProvider>
    );
  }
}

export default App;
