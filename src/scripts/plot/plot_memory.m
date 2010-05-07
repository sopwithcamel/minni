function plot_memory(filename)

% Parse the data
tmp_file = [filename, '.parsed'];
system(['python log_parse.py ', filename, ' ', tmp_file]);

Data = importdata(tmp_file);
Data(:,1) = Data(:,1).*10./60; % Tens of seconds to seconds
Data(:,2) = Data(:,2)./1000; % KB to MB
Data(:,3) = Data(:,3)./1000;

Cap = 2000;

figure(1);
clf;
hold on;
plot(Data(:,1), Data(:,2), '-r', 'LineWidth', 2.0);
plot(Data(:,1), Data(:,3),'-b', 'LineWidth', 2.0);
plot([min(Data(:,1)), max(Data(:,1))], [max(Data(:,2)), max(Data(:,2))], '--r');
plot([min(Data(:,1)), max(Data(:,1))], [max(Data(:,3)),max(Data(:,3))], '--b');
legend('Resident', 'Virtual', 'Location', 'Best');
title('Trace of Memory Consumption');
xlabel('Time (m)')
ylabel('Memory (MB)')